import { NestedNoteEvent } from "./nest.ts";
import { Anchor, UrlPrefixesKeys, PreferencesStore } from "./stores.ts";
import { decode } from "nostr-tools/nip19";
import { Filter } from "nostr-tools/filter";
import { Event } from "nostr-tools/core";
import { ShortTextNote, Metadata, Highlights, Reaction, Zap, Report, CommunityDefinition } from "nostr-tools/kinds";
import { matchAll, replaceAll } from "./nip27.ts";
import { Remarkable } from 'remarkable';
import { linkify } from 'remarkable/linkify';
import { findAll, save } from "./db.ts";
import { store } from "./stores.ts";
import { NoteEvent, Profile, Pk, Eid, ReactionEvent } from "./models.ts";
import { pool, rankRelays, manualLogin } from "./network.ts";
import { currentTime } from "./date-time.ts";
import { generatePicture } from "./picture.ts";

// Misc profile helpers

export const updateProfiles = async (pks: Set<Pk>, relays: string[], profiles: Profile[]): Promise<void> => {
  const pubkeys = new Set([...pks].filter(pk => !store.requestedProfileUpdate.has(pk)));
  pubkeys.forEach(pk => store.requestedProfileUpdate.add(pk));

  const kind = Metadata;
  const now = currentTime();
  const sixHours = 6 * 60 * 60;

  const _profiles: [Pk, Profile][] = profiles.map(p => [p.pk, p]);
  const _pkToProfile: Map<Pk, Profile> = new Map(_profiles);
  const pubkeysToUpdate = new Set([...pubkeys].filter(pubkey => {
    const profile = _pkToProfile.get(pubkey);
    if (profile?.l && profile!.l > now - sixHours) {
      // console.log(profile!.lastChecked, now - sixHours, profile!.lastChecked < now - sixHours);
      return false;
    } else {
      return true;
    }
  }).filter(e => !!e));

  if (pubkeysToUpdate.size === 0) {
    return;
  }

  const since = Math.min(..._profiles
    .filter(([pk, p]) => pubkeysToUpdate.has(pk))
    .map(([_, p]) => p.l ? p.l + 1 : 0));

  const { fastRelays, slowRelays } = await rankRelays([...relays, ...store.profileRelays], { kind });
  const filters = [{ kinds: [kind], authors: [...pubkeysToUpdate], since: since === Infinity ? 0 : since }];
  const update = async (relays: string[]) => {
    if (relays.length === 0) return;
    await pool.subscribeManyEose(relays, filters, {
      onevent(e: Event) {
        const pubkey = e.pubkey;
        if (!pubkeysToUpdate.has(pubkey)) return;
        try {
          const payload = JSON.parse(e.content);
          const updatedProfile = {
            pk: pubkey,
            ts: e.created_at,
            i: payload.image || payload.picture,
            n: payload.displayName || payload.display_name || payload.name,
          };
          const storedProfile = _pkToProfile.get(pubkey);
          const updated = !storedProfile || !storedProfile?.i || !storedProfile?.n || storedProfile!.ts < updatedProfile.ts;
          if (updated) {
            const newProfile = storedProfile
              ? { ...storedProfile, ...updatedProfile, l: now }
              : { ...updatedProfile, l: now };
            _pkToProfile.set(pubkey, newProfile);
            save('profiles', newProfile);
            console.log(`[zapthreads] updated profile ${pubkey}`);
          }
        } catch (err) {
          console.error(err);
        }
      },
    });
  };

  await update(fastRelays);
  update(slowRelays);
};

export const encodedEntityToFilter = (entity: string): Filter => {
  const decoded = decode(entity);
  switch (decoded.type) {
    case 'nevent': return {
      'kinds': [ShortTextNote],
      'ids': [decoded.data.id]
    };
    case 'note': return {
      'kinds': [ShortTextNote],
      'ids': [decoded.data]
    };
    case 'naddr': return {
      'kinds': [decoded.data.kind],
      'authors': [decoded.data.pubkey],
      "#d": [decoded.data.identifier]
    };
    default: return {};
  }
};

const BAD_NIP27_REGEX = /(?<=^|\s)@?((naddr|npub|nevent|note)[a-z0-9]{20,})/g;
const ANY_HASHTAG = /\B\#([a-zA-Z0-9]+\b)(?!;)/g;

export const parseContent = (e: NoteEvent, store: PreferencesStore, articles: NoteEvent[] = []): string => {
  let content = e.c;
  const urlPrefixes = store.urlPrefixes!;
  const profiles = store.profiles!;

  // turn hashtags into links (does not match hashes in URLs)
  const hashtags = [...new Set(e.t)];
  if (hashtags.length > 0) {
    const re = new RegExp(`(^|\\s)\\#(${hashtags.join('|')})`, 'gi');
    content = content.replaceAll(re, `$1[#$2](${urlPrefixes.tag}$2)`);
  }

  // NIP-27 attempts => NIP-27
  content = content.replaceAll(BAD_NIP27_REGEX, 'nostr:$1');

  // NIP-27 => Markdown
  content = replaceAll(content, ({ decoded, value }) => {
    switch (decoded.type) {
      case 'nprofile':
        let p1 = profiles().find(p => p.pk === decoded.data.pubkey);
        const text1 = p1?.n || shortenEncodedId(value);
        return `[@${text1}](${urlPrefixes.nprofile}${value})`;
      case 'npub':
        let p2 = profiles().find(p => p.pk === decoded.data);
        const text2 = p2?.n || shortenEncodedId(value);
        return `[@${text2}](${urlPrefixes.npub}${value})`;
      case 'note':
        return `[@${shortenEncodedId(value)}](${urlPrefixes.note}${value})`;
      case 'naddr':
        const d = decoded.data;
        const article = articles.find(a => a.pk === d.pubkey && a.d === d.identifier);
        if (article && article.tl) {
          return `[${article.tl}](${urlPrefixes.naddr}${value})`;
        }
        return `[@${shortenEncodedId(value)}](${urlPrefixes.naddr}${value})`;
      case 'nevent':
        return `[@${shortenEncodedId(value)}](${urlPrefixes.nevent}${value})`;
      default: return value;
    }
  });

  const md = new Remarkable('full', {
    linkTarget: '_blank',
    typographer: true,
    quotes: e.language === 'ru' ? '«»‘’' : '“”‘’',
  }).use(linkify);

  // Markdown => HTML
  return md.render(content.trim());
};

export const generateTags = (content: string): string[][] => {
  const result = [];
  // generate p and e tags in content
  const nostrMatches = matchAll(content);

  for (const m of nostrMatches) {
    if (m.decoded.type === 'npub') {
      result.push(['p', m.decoded.data]);
    }
    if (m.decoded.type === 'naddr') {
      const data = m.decoded.data;
      result.push(['a', `${data.kind}:${data.pubkey}:${data.identifier}`, '', 'mention']);
    }
    if (m.decoded.type === 'nevent') {
      result.push(['e', m.decoded.data.id]);
    }
    if (m.decoded.type === 'note') {
      result.push(['e', m.decoded.data]);
    }
  }

  // add t tags from hashtags in content
  const hashtagMatches = content.matchAll(ANY_HASHTAG);
  const hashtags = new Set([...hashtagMatches].map(m => m[1].toLowerCase()));
  for (const t of hashtags) {
    result.push(['t', t]);
  }

  return result;
};

export const parseUrlPrefixes = (value: string = '') => {
  const result: { [key in UrlPrefixesKeys]?: string; } = {
    naddr: 'https://nostr.at/',
    npub: 'https://nostr.at/',
    nprofile: 'https://nostr.at/',
    nevent: 'https://nostr.at/',
    note: 'https://nostr.at/',
    tag: 'https://snort.social/t/',
    picture: '',
  };

  for (const pair of value.split(',')) {
    const [key, value] = pair.split(':');
    result[key as UrlPrefixesKeys] = value ? `https://${value}` : '';
  }
  return result;
};

export const shortenEncodedId = (encoded: string) => {
  return encoded.substring(0, 8) + '...' + encoded.substring(encoded.length - 4);
};

export const svgWidth = 20;
const defaultPicture = 'data:image/svg+xml;utf-8,<svg viewBox="0 0 1024 1024" xmlns="http://www.w3.org/2000/svg"><circle cx="512" cy="512" r="512" fill="%23333" fill-rule="evenodd" /></svg>';

export const picturePlaceholder = (pk?: Pk) => {
  if (!pk) {
    return defaultPicture;
  }

  const p = store.urlPrefixes!.picture;
  if (p) {
    const url = new URL(p);
    url.pathname = pk;
    return url.href;
  } else {
    return generatePicture(pk);
  }
};

export const satsAbbrev = (sats: number): string => {
  if (sats < 10000) {
    return sats.toString();
  } else if (sats < 1000000) {
    return Math.round(sats / 1000) + 'k';
  } else {
    return Math.round(sats / 1000000) + 'M';
  }
};

export const totalChildren = (event: NestedNoteEvent): number => {
  return event.children.reduce<number>((acc, c) => {
    return acc + totalChildren(c);
  }, event.children.length);
};

const removeSlashesRegex = /\/+$/;

export const normalizeURL = (url: string, removeSlashes: boolean = true): string => {
  const u = new URL(url);
  u.hash = "";
  if (removeSlashes) {
    u.pathname = u.pathname.replace(removeSlashesRegex, '');
  }
  return u.toString();
};

export const errorText = <T>(exception: T) => {
  const err = (exception as any);
  return err?.message || String(err?.reason);
};

export const getSigner = async () => {
  try {
    return await manualLogin();
  } catch (e) {
    console.error(errorText(e));
  }
};
