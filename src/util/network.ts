import { modifyMutable, produce } from "solid-js/store";
import { UnsignedEvent, Event, Nostr, VerifiedEvent } from "nostr-tools/core";
import { normalizeURL as nostrNormalizeURL } from "nostr-tools/utils";
import { Filter, matchFilter } from "nostr-tools/filter";
import { SubCloser, SubscribeManyParams as SubscribeManyParamsDefault } from "nostr-tools/pool";
import { AbstractRelay as AbstractRelay, SubscriptionParams, Subscription, type AbstractRelayConstructorOptions } from "nostr-tools/abstract-relay";
import { getEventHash, verifyEvent } from "nostr-tools/pure";
import { Relay, RelayRecord } from "nostr-tools/relay";
import { ShortTextNote, Metadata, Highlights, Reaction, Report, CommunityDefinition, Zap, RelayList, Contacts, Mutelist, isReplaceableKind } from "nostr-tools/kinds";
import { RelayInformation } from "nostr-tools/nip11";
import { getPow, minePow } from "nostr-tools/nip13";
import { npubEncode } from "nostr-tools/nip19";
import { find, findAll, save, onSaved, remove } from "./db.ts";
import { EventSigner, store, signersStore } from "./stores.ts";
import { Eid, RelayInfo, RelayStats, Pk, eventToNoteEvent, NoteEvent, parseClient, Session } from "./models.ts";
import { NestedNoteEvent } from "./nest.ts";
import { medianOrZero, maxBy } from "./collections.ts";
import { currentTime, MIN_IN_SECS, DAY_IN_SECS, WEEK_IN_SECS, SIX_HOURS_IN_SECS } from "./date-time.ts";
import { errorText, parseContent, totalChildren, getSigner } from "./ui.ts";
import { updateBlockFilters, loadBlockFilters, applyBlock } from "./block-lists.ts";
import { waitNostr } from "nip07-awaiter";
import { sha256 } from "@noble/hashes/sha256";
import { bytesToHex } from "@noble/hashes/utils";

export const NOTE_KINDS = [ShortTextNote, Highlights];
export const CONTENT_KINDS = [...NOTE_KINDS, Reaction, Zap];

type SubscribeManyParams = SubscribeManyParamsDefault & { oneoseOnRelay?: (relay: string) => void; };

const STATS_SIZE = 5;
const LONG_TIMEOUT = 7000;
const SHORT_TIMEOUT = Math.max(LONG_TIMEOUT * 0.8, LONG_TIMEOUT - 1000);

class PrioritizedPool {
  private relays = new Map<string, AbstractRelay>()
  public seenOn: Map<Eid, Set<string>> = new Map()

  public verifyEvent: Nostr['verifyEvent']

  private _WebSocket?: typeof WebSocket
  private eventsCount: { [relay: string]: number; } = {};
  private eventsTs: { [key: string]: number; } = {};

  constructor(opts: AbstractRelayConstructorOptions) {
    this.verifyEvent = opts.verifyEvent
    this._WebSocket = opts.websocketImplementation
  }

  async ensureRelay(url: string): Promise<AbstractRelay> {
    let relay = this.relays.get(url)
    if (!relay) {
      relay = new AbstractRelay(url, {
        verifyEvent: this.verifyEvent,
        websocketImplementation: this._WebSocket,
      });
      this.relays.set(url, relay);
    }
    // The connection timeout is now passed per connect() call, and relays
    // that require NIP-42 AUTH to read are detected from the subscription
    // close reason (see handleClose) — nostr-tools no longer exposes the
    // connectionTimeout property or the private _onauth hook the previous
    // detection relied on.
    await relay.connect({ timeout: SHORT_TIMEOUT })

    return relay
  }

  close(relays: string[]) {
    relays.forEach(url => {
      this.relays.get(url)?.close()
    })
  }

  async updateRelays(rawRelays?: string) {
    const relaysAreOk = (externalRelays: RelayRecord) => {
      const entries = Object.values(externalRelays);
      return !!(entries.find(i => i.read) && entries.find(i => i.write));
    };

    let requestedFromProfile = false;
    const loggedIn = !!(signersStore.active && window.nostr);
    let externalRelays: RelayRecord = {};

    const reloadFromProfile = async () => {
      const pk = signersStore.active!.pk;
      const lastEvent = await this.queryProfileEvent({ authors: [pk], kinds: [RelayList] });
      const tags = lastEvent ? lastEvent.tags : [];
      const externalRelays = Object.fromEntries(
       tags
        .filter(t => t.length >= 2 && t[0] === 'r')
        .map(t => {
          let entry = { read: true, write: true };
          if (t.length === 3) {
            const read = t[2] === 'read';
            entry[!read ? 'read' : 'write'] = false;
          }
          return [t[1], entry];
        }));
      if (relaysAreOk(externalRelays)) {
        save('profileRelays', { pk, relays: JSON.stringify(externalRelays) });
      }
      return externalRelays;
    };

    if (loggedIn) {
      const pk = signersStore.active!.pk;
      externalRelays = window.nostr!.getRelays ? await window.nostr!.getRelays() : {};
      if (!relaysAreOk(externalRelays)) {
        const profileRelays = await find('profileRelays', IDBKeyRange.only(pk));
        if (profileRelays) {
          externalRelays = JSON.parse(profileRelays.relays);
        }
      }

      if (!relaysAreOk(externalRelays)) {
        requestedFromProfile = true;
        externalRelays = await reloadFromProfile();
      }
    }

    const splitRelays: RelayRecord = loggedIn && relaysAreOk(externalRelays)
      ? externalRelays
      : Object.fromEntries(
          (rawRelays || '')
            .split(',')
            .map(r => [r, {read: true, write: loggedIn}]));

    const relays: RelayRecord = Object.fromEntries(
      Object
       .entries(splitRelays)
       .map(([relayUrl, options]: [string, any]) => [relayUrl.trim(), options])
       .filter(([relayUrl, options]) => relayUrl.length > 0)
       .map(([relayUrl, options]) => [nostrNormalizeURL(new URL(relayUrl).toString()), options]));

    const entries = Object.entries(relays);
    const readRelays = entries.filter(([relayUrl, options]) => options.read).map(([r, _]) => r);
    modifyMutable(store, produce((store) => {
      store.writeRelays = entries.filter(([relayUrl, options]) => options.write).map(([r, _]) => r);
      if (JSON.stringify(store.readRelays) !== JSON.stringify(readRelays)) {
        store.readRelays = readRelays;
      }
    }));
    console.log(`[zapthreads] readRelays=${JSON.stringify(store.readRelays)} writeRelays=${JSON.stringify(store.writeRelays)} profileRelays=${JSON.stringify(store.profileRelays)}`);
    await this.updateWritePow();

    if (loggedIn && !requestedFromProfile) {
      reloadFromProfile();
    }
  }

  async subscribeMany(relays: string[], filters: Filter[], params: SubscribeManyParams): Promise<SubCloser> {
    return await this.subscribeManyMap(Object.fromEntries(relays.map(url => [url, filters])), params)
  }

  async subscribeManyMap(rawRequests: { [relay: string]: Filter[] }, params: SubscribeManyParams): Promise<SubCloser> {
    const { fastRelays, slowRelays, ranks } = await rankRelays(Object.keys(rawRequests));
    const fastOrSlowRelays = new Set([...fastRelays, slowRelays]);

    const requests = [];
    for (const [relayUrl, filters] of Object.entries(rawRequests)) {
      if (!fastOrSlowRelays.has(relayUrl)) continue;
      const newFilters: Filter[] = [];
      for (const f of filters) {
        newFilters.push({ ...f, since: await this.getSince(relayUrl, f) });
      }
      requests.push({
        url: relayUrl,
        filters: newFilters,
        maxSince: Math.max(...newFilters.map(f => f.since!)),
        latency: ranks.get(relayUrl)!,
      });
    }
    console.log(`[zapthreads] subscribing to ${requests.length} relays (${Object.keys(rawRequests).length - requests.length} failing)`);

    requests.sort((a, b) => {
      if (a.latency !== b.latency) return a.latency - b.latency;
      if (a.maxSince !== b.maxSince) return a.maxSince - b.maxSince;
      return 0; // don't sort if it's initial warmup
    });

    params.receivedEvent = (relay: AbstractRelay, id: string) => {
      let set = this.seenOn.get(id)
      if (!set) {
        set = new Set()
        this.seenOn.set(id, set)
      }
      set.add(relay.url)
      this.eventsCount[relay.url] = (this.eventsCount[relay.url] || 0) + 1;
    }

    const _knownIds = new Set<string>()
    const subs: Subscription[] = []

    // batch all EOSEs into a single
    const eosesReceived: boolean[] = []
    let handleEose = (i: number) => {
      eosesReceived[i] = true;
      if (eosesReceived.filter(a => a).length === requests.length) {
        params.oneose?.();
        handleEose = () => {};
      }
    }
    // batch all closes into a single
    const closesReceived: string[] = [];
    let handleClose = (i: number, url: string, reason: string) => {
      if (reason === 'connection timed out' || reason === 'websocket error') {
        addRelayStats(url, Infinity);
      } else if (reason.includes('blocked') || reason === 'relay connection errored') {
        addRelayStats(url, Infinity, -1, true);
      } else if (reason.includes('auth')) {
        this.updateRelayInfo(url, { readAuth: true });
      } else if (reason.includes('write-only')) {
        this.updateRelayInfo(url, { writeOnly: true });
      }

      handleEose(i);
      closesReceived[i] = reason;
      if (closesReceived.filter(a => a).length === requests.length) {
        params.onclose?.(closesReceived);
        handleClose = () => {};
      }
    }

    const localAlreadyHaveEventHandler = (id: string) => {
      if (params.alreadyHaveEvent?.(id)) {
        return true
      }
      const have = _knownIds.has(id)
      _knownIds.add(id)
      return have
    }

    const tasks = requests.map(async ({ url, filters, latency: predictedTime }, i) => {
      console.log(`[zapthreads] subscribing on ${url} with estimated latency ${Math.floor(predictedTime)} ms`);
      let relay: AbstractRelay
      try {
        relay = await this.ensureRelay(url);
      } catch (err) {
        const reason = errorText(err) || '';
        console.log(`[zapthreads] ${url} closed ${reason}`);
        handleClose(i, url, reason);
        return;
      }

      const startEventsCount = this.eventsCount[url] || 0;
      const startTime = Date.now();
      const subscription = relay.subscribe(filters, {
        ...params,
        onevent: (event: Event) => {
          filters
            .filter(f => matchFilter(f, event))
            .forEach(f => this.saveSince(url, f, event.created_at));
          params.onevent?.(event);
        },
        oneose: () => {
          const deltaTime = Date.now() - startTime;
          const deltaEventsCount = (this.eventsCount[url] || 0) - startEventsCount;
          delete this.eventsCount[url];
          const averageLatency = deltaTime / Math.max(1, deltaEventsCount);
          addRelayStats(url, averageLatency);
          params.oneoseOnRelay?.(url);
          handleEose(i);
        },
        onclose: reason => handleClose(i, url, reason),
        alreadyHaveEvent: localAlreadyHaveEventHandler,
        eoseTimeout: params.maxWait,
      });

      subs.push(subscription)
    });

    // open a subscription in all given relays
    const startTime = Date.now();
    const allOpened = Promise.allSettled(tasks).then(_ => {
      console.log(`[zapthreads] processed subscriptions in ${Date.now() - startTime} ms`);
    });

    return {
      async close() {
        await allOpened
        // TODO: await profile updates and publishing to slow relays?
        subs.forEach(sub => {
          sub.close()
        })
      },
    }
  }

  async subscribeManyEose(
    relays: string[],
    filters: Filter[],
    params: Pick<SubscribeManyParams, 'id' | 'onevent' | 'onclose' | 'maxWait'>,
  ): Promise<SubCloser> {
    const fifteenMins = 15*60;
    const until = currentTime() + fifteenMins;
    const subcloser = await this
      .subscribeMany(
        relays,
        filters.map(f => f.until ? f : { ...f, until }), {
          ...params,
          oneose() {
            subcloser.close()
          },
        }
      );
    return subcloser
  }

  async querySync(
    relays: string[],
    filter: Filter,
    params?: Pick<SubscribeManyParams, 'id' | 'maxWait'>,
  ): Promise<Event[]> {
    return new Promise(async resolve => {
      const events: Event[] = []
      await this.subscribeManyEose(relays, [filter], {
        ...params,
        onevent(event: Event) {
          events.push(event)
        },
        onclose(_: string[]) {
          resolve(events)
        },
      })
    })
  }

  async queryLast(
    relays: string[],
    filter: Filter,
    params?: Pick<SubscribeManyParams, 'id' | 'maxWait'>,
  ): Promise<Event | undefined> {
    const events = await this.querySync(relays, filter, params);
    return maxBy(events, e => e.created_at);
  }

  async queryProfileEvent(
    filter: Filter,
    params?: Pick<SubscribeManyParams, 'id' | 'maxWait'>,
  ): Promise<Event | undefined> {
    let relays = [...store.readRelays, ...store.writeRelays, ...store.profileRelays];
    if (signersStore.active) {
      const profileRelays = await find('profileRelays', IDBKeyRange.only(signersStore.active.pk));
      if (profileRelays) {
        relays = [...relays, ...Object.keys(JSON.parse(profileRelays.relays))];
      }
    }
    return await this.queryLast(relays, filter, params);
  }

  async publishEvent(event: Event): Promise<{ ok: number, failures: number }> {
    let writeRelays = store.writeRelays;
    if (writeRelays.length === 0) {
      await this.updateRelays();
    }
    writeRelays = store.writeRelays;
    const { fastRelays, slowRelays, offlineRelays, unsupported } = await rankRelays(writeRelays, { event, write: true });

    console.log(
      `[zapthreads] publishing to ${fastRelays.length + slowRelays.length} relays` +
      (unsupported + offlineRelays > 0 ? ` (ignored ${unsupported} unsupported, ${offlineRelays} failing)` : ''));

    let concurrent = true;
    if (store.onPublish) {
      const onPublishResult = await store.onPublish({ relays: [...fastRelays, ...slowRelays] });
      if (!onPublishResult.accepted) {
        return { ok: 0, failures: 0 };
      } else if (onPublishResult.concurrent !== undefined) {
        concurrent = onPublishResult.concurrent!;
      }
    }

    const result = await this.publish(event, fastRelays, concurrent);
    this.publish(event, slowRelays, concurrent);

    return result;
  }

  private async publish(event: Event, relays: string[], concurrent: boolean): Promise<{ ok: number, failures: number }> {
    if (relays.length === 0) return { ok: 0, failures: 0 };

    if (concurrent) {
      return await this.concurrentPublish(event, relays);
    } else {
      return await this.sequentialPublish(event, relays);
    }
  }

  private async concurrentPublish(event: Event, relays: string[]): Promise<{ ok: number, failures: number }> {
    const startTime = Date.now();
    const tasks = relays.map(async (relayUrl) => await this.publishOnRelay(relayUrl, event));
    const results = await Promise.allSettled(tasks);
    const deltaTime = Date.now() - startTime;

    const { ok, failures } = countResults(results, relays);
    console.log(`[zapthreads] event ${event.id} published concurrently in ${deltaTime} ms to ${ok} relays (${failures} failed)`, relays);
    return { ok, failures };
  }

  private async sequentialPublish(event: Event, relays: string[]): Promise<{ ok: number, failures: number }> {
    let ok = 0;
    let failures = 0;
    const startTime = Date.now();
    for (const relayUrl of relays) {
      try {
        await this.publishOnRelay(relayUrl, event);
        ok++;
      } catch (err) {
        console.error(err);
        failures++;
      }
    }
    const deltaTime = Date.now() - startTime;

    console.log(`[zapthreads] event ${event.id} published sequentially in ${deltaTime} ms to ${ok} relays (${failures} failed)`, relays);
    return { ok, failures };
  }

  public async estimateWriteRelayLatencies() {
    const test = async (relayUrl: string) => {
      if (this.relays.get(relayUrl)?.connected) return;
      const stats = (await findAll('relayStats', relayUrl, { index: 'name' }));
      if (stats.length >= STATS_SIZE) return;
      const startTime = Date.now();
      let deltaTime = Infinity;
      try {
        const relay = await this.ensureRelay(relayUrl);
        deltaTime = Date.now() - startTime;
      } finally {
        addRelayStats(relayUrl, deltaTime);
      }
    };
    const tasks = store.writeRelays.map(async (relayUrl) => await test(relayUrl));
    await Promise.allSettled(tasks);
  }

  private async publishOnRelay(relayUrl: string, event: Event) {
    const startTime = Date.now();
    let deltaTime = Infinity;
    try {
      const relay = await this.ensureRelay(relayUrl);
      await relay.publish(event);
      deltaTime = Date.now() - startTime;
    } finally {
      addRelayStats(relayUrl, deltaTime, event.kind);
    }
  }

  async updateRelayInfos() {
    if (store.disableFeatures!.includes('relayInformation')) return;
    const now = currentTime();
    const expiredRelays = [];
    const { fastRelays, slowRelays, offlineRelays } = await rankRelays([...store.readRelays, ...store.writeRelays]);
    for (const relayUrl of [...fastRelays, ...slowRelays]) {
      if (this.relays.get(relayUrl)?.connected) {
        const relayInfo = await find('relayInfos', IDBKeyRange.only(relayUrl));
        if (infoExpired(now, relayInfo)) {
          expiredRelays.push(relayUrl);
        }
      }
    }

    if (expiredRelays.length === 0) return;
    const results = await Promise.allSettled(expiredRelays
      .map(async (relayUrl) => {
        let info;
        try {
          const possibleInfo = await fetchRelayInformation(relayUrl);

          // ensure server returned something parsable
          supportedReadRelay(possibleInfo);
          supportedWriteRelay(undefined, possibleInfo);

          info = possibleInfo;
        } catch (err) {
          const reason = (err as any)?.message || String(err);
          console.info(`[zapthreads] info for ${relayUrl} failed with "${reason}"`);
        }

        await save('relayInfos', {
          name: relayUrl,
          info,
          l: now,
        });
      }));

    const { ok, failures } = countResults(results, expiredRelays);
    console.log(`[zapthreads] updated infos for ${ok} relays (${failures} failed)`);
    await onSaved(async () => await this.updateWritePow());
  }

  private async updateRelayInfo(relayUrl: string, newRelayInfo: any) {
    const relayInfo = await find('relayInfos', IDBKeyRange.only(relayUrl)) || {};
    await save('relayInfos', {
      ...relayInfo,
      ...newRelayInfo,
      name: relayUrl,
    });
  }

  private async updateWritePow() {
    const writeRelaysPows = await Promise.all(store.writeRelays.map(async (relayUrl) => {
      const info = (await find('relayInfos', IDBKeyRange.only(relayUrl)))?.info;
      return info?.limitation?.min_pow_difficulty || 0;
    }));
    store.writePowDifficulty = Math.max(store.minReadPow, ...writeRelaysPows);
  }

  private async getSince(relayUrl: string, filter: Filter) {
    if (filter.since !== undefined) return filter.since!;
    if (!store.anchor) return 0;

    const key = filterCacheKey(relayUrl, filter);
    if (this.eventsTs[key] !== undefined) return this.eventsTs[key];

    const relay = await find('requestFilters', IDBKeyRange.only(key));
    if (!relay) return 0;

    const hour = 3600;
    const now = currentTime();
    const updatedLongTimeAgo = relay.l + hour < now;
    const since = Math.max(0, updatedLongTimeAgo ? relay.l + 1 : relay.l - hour); // make sure we don't miss events sent from machines with poor network or poor time sync
    return since;
  }

  private saveSince(relayUrl: string, filter: Filter, ts: number) {
    if (!store.anchor || filter.kinds?.filter(k => isReplaceableKind(k))) return;
    const key = filterCacheKey(relayUrl, filter);
    const since = Math.max(this.eventsTs[key] || 0, ts);
    this.eventsTs[key] = since;
    save('requestFilters', { key, l: since });
  }
}

export const pool = new PrioritizedPool({ verifyEvent, websocketImplementation: undefined });

const addRelayStats = async (relayUrl: string, latency: number, kind: number = -1, overwrite: boolean = false) => {
  const stats = (await findAll('relayStats', relayUrl, { index: 'name' }))
    .filter(stats => stats.kind === kind);
  const lastStat = maxBy(stats, s => s.ts);
  save('relayStats', {
    name: relayUrl,
    kind,
    serial: lastStat ? (lastStat.serial + (overwrite ? 0 : 1)) % STATS_SIZE : 0,
    latency,
    ts: currentTime(),
  });
}

export const powIsOk = (id: Eid, powOrTags: number | string[][], minPow: number): boolean => {
  if (minPow === 0) {
    return true;
  }

  let pow = 0;
  if (typeof powOrTags === 'number') {
    pow = powOrTags;
  } else if (typeof powOrTags === 'object') {
    const nonce = powOrTags.find(t => t.length > 2 && t[0] === 'nonce');
    pow = nonce && +nonce[2] || 0;
  }
  return pow >= minPow && getPow(id) >= minPow;
};

const writeOnlyRelay = (relayInfo?: RelayInfo) => relayInfo?.writeOnly === true;

const supportedReadRelay = (info?: RelayInformation, relayInfo?: RelayInfo) => {
  //if (relayInfo?.readAuth === true) return false;
  if (!info) return true;

  const language = store.language;
  if (language && info.language_tags && info.language_tags.length > 0 && !info.language_tags!.includes(language)) return false;

  return true;
};

const supportedWriteRelay = (event?: Event, info?: RelayInformation) => {
  if (!info) return true;
  if (!supportedReadRelay(info)) return false;

  /* TODO: enable when more relays will report they support NIP-25
  const requiredNips = event.kind === Reaction ? [25] : [];
  if (info.supported_nips.length > 0 && requiredNips.filter(n => info.supported_nips.includes(n)).length !== requiredNips.length) {
    return false;
  }*/

  const retention = info.retention;
  if (retention) {
    const eventKind = event ? event.kind : 1;
    const allowed = retention.filter(r => {
      const disallowed = (r.time && r.time === 0) || (r.count && r.count === 0);
      const kindMatches = r.kinds && r.kinds.includes(eventKind);
      const kindRangeMatches = r.kinds && r.kinds
        .filter(r => Array.isArray(r))
        .map(r => r as number[])
        .map(kindRange => kindRange.length == 2 && eventKind >= kindRange[0] && eventKind <= kindRange[1])
        .length > 0;
      return disallowed && (kindMatches || kindRangeMatches);
    }).length === 0;
    if (!allowed) {
      return false;
    }
  }

  const limitation = info.limitation;
  const maxWritePow = store.maxWritePow;
  if (limitation) {
    if (limitation.auth_required && limitation.auth_required!) return false;
    if (limitation.payment_required && limitation.payment_required!) return false;
    if (limitation.min_pow_difficulty && ((maxWritePow && maxWritePow < limitation.min_pow_difficulty) || (event && !powIsOk(event.id, event.tags, limitation.min_pow_difficulty)))) return false;
    if (limitation.max_content_length && event && event.content.length > limitation.max_content_length) return false;
    if (limitation.max_message_length && event && ('["EVENT",' + JSON.stringify(event) + ']').length > limitation.max_message_length) return false;
  }

  return true;
};

export const rankRelays = async (relays: string[], options?: { kind?: number; event?: Event; write?: boolean; }) => {
  const now = currentTime();

  let kind: number = -1;
  if (options && options.kind) {
    kind = options.kind;
  } else if (options && options.event) {
    kind = options.event.kind;
  }

  const write: boolean = options?.write || false;

  let fastRelays = [];
  const slowRelays = [];
  let offlineRelays = 0;
  let unsupported = 0;
  const ranks = new Map;

  for (const relayUrl of new Set(relays)) {
    const relayInfo = await find('relayInfos', IDBKeyRange.only(relayUrl));
    if (relayInfo) {
      try {
        if (
          (!write && (!supportedReadRelay(relayInfo?.info, relayInfo) || writeOnlyRelay(relayInfo))) ||
          (write && !supportedWriteRelay(options?.event, relayInfo?.info))
        ) {
          unsupported++;
          continue;
        }
      } catch (_) { }
    }

    const stats = (await findAll('relayStats', relayUrl, { index: 'name' }));
    const generalLatency = medianOrZero(stats.map(s => s.latency));

    const exactStats = stats.filter(stats => stats.kind === kind);
    const exactLatency = medianOrZero(exactStats.map(s => s.latency));

    const lastStat = exactStats.length > 0
      ? maxBy(exactStats, s => s.ts)
      : maxBy(stats, s => s.ts);

    const latency = exactStats.length > 0 ? exactLatency : generalLatency;
    ranks.set(relayUrl, latency);
    if (latency === Infinity) {
      if (lastStat && lastStat.latency === Infinity && now < lastStat.ts + 0.5 * MIN_IN_SECS) {
        offlineRelays++;
      } else {
        slowRelays.push(relayUrl);
      }
    } else {
      fastRelays.push({ relayUrl, latency });
    }
  }

  fastRelays = fastRelays
    .sort((a, b) => a.latency - b.latency)
    .map(({ relayUrl }) => relayUrl);

  if (fastRelays.length === 0) {
    return { fastRelays: slowRelays, slowRelays: [], offlineRelays: 0, ranks, unsupported };
  }

  return { fastRelays, slowRelays, offlineRelays, unsupported, ranks };
};

export const fetchRelayInformation = async (url: string): Promise<RelayInformation | undefined> => {
  if (store.disableFeatures!.includes('relayInformation')) return;
  return (await (
    await shortFetch(url.replace('ws://', 'http://').replace('wss://', 'https://'), {
      headers: { Accept: 'application/nostr+json' },
    })
  ).json()) as RelayInformation;
};

export const infoExpired = (now: number, relayInfo?: RelayInfo) => {
  const lastInfoUpdateAttemptAt = relayInfo && relayInfo.l;
  return !lastInfoUpdateAttemptAt || now > lastInfoUpdateAttemptAt + WEEK_IN_SECS;
};

const countResults = <T>(results: PromiseSettledResult<T>[], relays: string[]) => {
  let ok = 0;
  let failures = 0;
  for (let i = 0; i < results.length; i++) {
    if (results[i].status === 'fulfilled') {
      ok++;
    } else {
      const err = (results[i] as any)?.reason;
      console.error(relays[i], err?.message || String(err));
      failures++;
    }
  }
  return { ok, failures };
};

export const shortFetch = (url: string, options?: any) => fetch(url, {
  ...options,
  signal: AbortSignal.timeout(SHORT_TIMEOUT),
});

export const sign = async (unsignedEvent: UnsignedEvent, signer: EventSigner) => {
  if (store.client && unsignedEvent.tags.filter(tag => tag.length > 0 && tag[0] === 'client').length === 0) {
    unsignedEvent.tags.push(['client', store.client]);
  }

  const pow = store.writePowDifficulty;
  let event: Event;
  if (pow > 0) {
    const eventWithPow = minePow(unsignedEvent, pow);
    const signature = await signer.signEvent!(eventWithPow);
    event = { ...eventWithPow, ...signature };
  } else {
    const id = getEventHash(unsignedEvent);
    const signature = await signer.signEvent!(unsignedEvent);
    event = { id, ...unsignedEvent, ...signature };
  }
  console.log(JSON.stringify(event, null, 2));
  return event;
};

export const signAndPublishEvent = async (unsignedEvent: UnsignedEvent, signer: EventSigner): Promise<{ ok: number, failures: number, event: Event }> => {
  const event = await sign(unsignedEvent, signer);
  const { ok, failures } = await pool.publishEvent(event);
  return { ok, failures, event };
};

export const loginIfKnownUser = async () => {
  waitNostr(0);

  const session = await find('sessions', +true, { index: 'autoLogin' });
  if (!session) return;

  const nostr = await waitNostr(1000);
  if (!nostr) {
    console.log('[zapthreads] nip-07 is probably removed');
    return;
  }

  const pubkey = await nostr.getPublicKey();
  if (pubkey !== session.pk) {
    console.log('[zapthreads] unknown user, disabling auto-login');
    save('sessions', { ...session, autoLogin: +false });
    return;
  }

  if (store.onLogin && !(await store.onLogin({ knownUser: true }))) return;

  signersStore.active = {
    pk: pubkey,
    signEvent: async (event: UnsignedEvent) => await nostr.signEvent(event),
  };

  store.subscribed(session.subscriber);
};

export const manualLogin = async () => {
  if (signersStore.active) return signersStore.active;

  let knownUser = false;
  let pk: Pk | undefined;
  const sessions: Session[] = await findAll('sessions', +false, { index: 'autoLogin' });
  if (sessions.length > 0) {
    if (window.nostr) {
      console.log('[zapthreads] nip-07 became available, trying to re-enable auto-login');
      try {
        pk = await window.nostr!.getPublicKey();
      } catch (err) {
        console.error(err);
        console.log('[zapthreads] cannot retrieve pubkey, removing old sessions');
        // @ts-ignore
        remove('sessions', sessions.map(s => s.pk));
        store.subscribed(false);
      }
      const session = sessions.find(s => s.pk === pk);
      if (session) {
        knownUser = true;
        console.log('[zapthreads] re-enabling auto-login');
        store.subscribed(session.subscriber);
      }
    } else {
      console.log('[zapthreads] nip-07 is probably removed');
      const autoLoginSessions = await findAll('sessions', +false, { index: 'autoLogin' });
      if (autoLoginSessions.length > 0) console.log('[zapthreads] disabling auto-login');
      autoLoginSessions.forEach(session => save('sessions', { ...session, autoLogin: +false }));
      store.subscribed(false);
    }
  }

  const { accepted, autoLogin } = store.onLogin
    ? await store.onLogin({ knownUser })
    : { accepted: true, autoLogin: false };
  if (!accepted) {
    if (pk) {
      console.log('[zapthreads] removing session', pk);
      // @ts-ignore
      remove('sessions', [pk]);
      store.subscribed(false);
    }
    return;
  }

  if (!window.nostr) {
    throw new Error('No NIP-07 extension!');
  }

  pk ||= await window.nostr!.getPublicKey();

  signersStore.active = {
    pk,
    signEvent: async (event: UnsignedEvent) => await window.nostr!.signEvent(event),
  };

  if (!signersStore.active.signEvent) {
    console.error('User has no signer!');
  }

  if (autoLogin === true || knownUser) {
    console.log('[zapthreads] saving session');
    save('sessions', {
      pk,
      autoLogin: +true,
      subscriber: store.subscribed(),
    });
  }

  await pool.updateRelays();
  return signersStore.active;
};

export const logout = async () => {
  if (!signersStore.active) return;
  const session = await find('sessions', IDBKeyRange.only(signersStore.active.pk));
  const pk = session?.pk;
  if (pk) {
    // @ts-ignore
    remove('sessions', [pk]);
    store.subscribed(false);
  }
  signersStore.active = undefined;
};

export const validateWriteEvent = (e: UnsignedEvent & { id: undefined } | Event, minReadPow?: number) => {
  const upvotes = 0;
  const downvotes = 0;
  const rankable = false;
  if (NOTE_KINDS.includes(e.kind)) {
    return validateNestedNoteEvent({ e: { ...eventToNoteEvent(e), children: [] }, rankable, minReadPow, upvotes, downvotes });
  };

  preValidate({ id: e.id, pubkey: e.pubkey, kind: e.kind, powOrTags: e.tags, content: e.content }, minReadPow);
  const result = store.onEvent
    ? store.onEvent({
      rankable,
      kind: e.kind,
      content: e.content,
      client: parseClient(e.tags),
      replies: 0,
      upvotes,
      downvotes,
      pow: store.writePowDifficulty,
      followedByModerator: store.lists.pubkeysFollowed.has(e.pubkey),
    })
    : { sanitizedContent: e.content, rank: undefined, showReportButton: false };
  return { ...result, noteEvent: undefined };
};

export const validateNestedNoteEvent = (args: { e: NestedNoteEvent; rankable: boolean; minReadPow?: number; upvotes: number; downvotes: number; }) => {
  const { e, rankable, minReadPow, upvotes, downvotes } = args;
  const replies = totalChildren(e);
  preValidate({ id: e.id, pubkey: e.pk, kind: e.k, powOrTags: e.pow, content: e.c }, minReadPow);
  const result = store.onEvent
    ? store.onEvent({ rankable, kind: e.k, content: parseContent(e, store), replies, upvotes, downvotes, client: e.client, language: e.language, pow: e.pow, followedByModerator: store.lists.pubkeysFollowed.has(e.pk) })
    : { sanitizedContent: e.c, rank: undefined, showReportButton: false }
  return { ...result, noteEvent: e };
};

const preValidate = (e: { id: Eid | undefined; pubkey: Pk; kind: number; powOrTags: number | string[][]; content: string }, minReadPow?: number) => {
  if (!store.disableFeatures!.includes('followIsApproval') && store.lists.pubkeysFollowed.has(e.pubkey)) return;

  const blockedEvent = e.id && store.lists.eventsBlocked.has(e.id);
  if (blockedEvent) {
    applyBlock('eventsBlocked', e.id!);
  }

  const blockedPk = store.lists.pubkeysBlocked.has(e.pubkey);
  if (blockedPk) {
    applyBlock('pubkeysBlocked', e.pubkey);
  }

  if (blockedEvent) throw new Error('Block-listed event');
  if (blockedPk) throw new Error('Block-listed user');

  if (!e.content.trim() && e.content.length > store.maxCommentLength) throw new Error('Invalid message length');

  if (e.id !== undefined && minReadPow !== undefined && !powIsOk(e.id, e.powOrTags, minReadPow)) throw new Error('NIP-13 PoW is not okay');
};

const filterCacheKey = (relayUrl: string, filter: Filter) => {
  const ignored = ['since', 'until', 'limit'];
  const value = Object.fromEntries(Object
   .entries(filter)
   .filter(([k, _]) => !ignored.includes(k))
   .sort());
  const items = [relayUrl, store.anchor!.value, value];
  return bytesToHex(sha256(JSON.stringify(items)));
};

export const fetchContactsAndUpdateSubscribed = async (): Promise<Event | undefined> => {
  if (!signersStore.active) return;
  const authorPk = store.externalAuthor || store.anchorAuthor;
  if (!authorPk) return;

  const contacts = await pool.queryProfileEvent({ kinds: [Contacts], authors: [signersStore.active!.pk] });
  if (!contacts) return;

  const subscribed = contacts
    .tags
    .filter(i => i.length >= 2 && i[0] === 'p')
    .map(t => t[1])
    .reverse()
    .includes(authorPk!);
  store.subscribed(subscribed);

  await updateSubscribed();
  return contacts;
};

export const toggleSubscribe = async () => {
  const signer = await getSigner();
  const authorPk = store.externalAuthor || store.anchorAuthor;
  if (!signer || !authorPk) return;
  const wasSubscribed = store.subscribed();
  const contacts = await fetchContactsAndUpdateSubscribed();
  if (store.subscribed() !== wasSubscribed) {
    console.log('[zapthreads] already toggled subscription');
    return;
  }

  const tags = contacts ? contacts.tags : [];
  if (wasSubscribed) {
    const index = tags.findIndex(i => i.length >= 2 && i[0] === 'p' && i[1] === authorPk!);
    tags.splice(index, 1);
  } else {
    tags.push(['p', authorPk!]);
  }

  const unsignedEvent = {
    kind: Contacts,
    created_at: currentTime(),
    content: '',
    pubkey: signer.pk,
    tags,
  };

  const id = getEventHash(unsignedEvent);
  const signature = await signer.signEvent!(unsignedEvent);
  const event = { id, ...unsignedEvent, ...signature };
  console.log(JSON.stringify(event, null, 2));
  const { ok, failures } = await pool.publishEvent(event);
  if (ok > 0) {
    store.subscribed(!wasSubscribed);
  }

  await updateSubscribed();
  console.log('[zapthreads] subscribed', store.subscribed());
};

const updateSubscribed = async () => {
  const subscribed = store.subscribed();
  const session = await find('sessions', IDBKeyRange.only(signersStore.active!.pk));
  if (session && session.subscriber !== subscribed) {
    save('sessions', {
      ...session,
      subscriber: subscribed,
    });
  }
};
