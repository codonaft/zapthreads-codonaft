import { Index, Show, createEffect, createComputed, createMemo, createSignal, onCleanup, batch, on } from "solid-js";
import { createVisibilityObserver } from "@solid-primitives/intersection-observer";
import { currentTime, defaultPicture, parseContent, shortenEncodedId, sortByDate, svgWidth, timeAgo, totalChildren } from "./util/ui.ts";
import { publishEvent } from "./util/network.ts";
import { ReplyEditor } from "./reply.tsx";
import { NestedNoteEvent } from "./util/nest.ts";
import { noteEncode, npubEncode } from "nostr-tools/nip19";
import { UnsignedEvent, Event } from "nostr-tools/core";
import { getEventHash, finalizeEvent } from "nostr-tools/pure";
import { Relay } from "nostr-tools/relay";
import { EventSigner, signersStore, store } from "./util/stores.ts";
import { NoteEvent, Profile, Pk, ReactionEvent, VoteKind, Eid, voteKind } from "./util/models.ts";
import { remove } from "./util/db.ts";

export const Thread = (props: { nestedEvents: () => NestedNoteEvent[]; articles: () => NoteEvent[]; votes: () => ReactionEvent[]; firstLevelComments?: () => number; }) => {
  const anchor = () => store.anchor!;
  const profiles = store.profiles!;
  const relays = () => store.relays!;

  const MIN_AUTO_COLLAPSED_THREADS = 3;
  const MIN_AUTO_COLLAPSED_COMMENTS = 5;
  const AUTO_UNCOLLAPSE_THREADS_TIMEOUT_MS = 3000;

  return <div class="ztr-thread">
    <Index each={sortByDate(props.nestedEvents())}>
      {
        (event) => {
          const [isOpen, setOpen] = createSignal(false);
          const [isExpanded, setExpanded] = createSignal(false);
          const [overflowed, setOverflowed] = createSignal(false);

          const [isThreadCollapsed, setThreadCollapsed] = createSignal(!event().parent);
          const [frozenAutoThreadCollapse, setFreezeAutoThreadCollapse] = createSignal(false);

          let commentRepliesElement: HTMLDivElement | undefined;
          const visible = createVisibilityObserver({ threshold: 0.0 })(() => commentRepliesElement);

          const [votesCount, setVotesCount] = createSignal(0);
          const [hasVotes, setHasVotes] = createSignal(false);
          const [currentUserVote, setCurrentUserVote] = createSignal(0);
          const currentNoteVotes = () => props.votes().filter(r => r.noteId === event().id);
          const currentNoteVotesDeduplicatedByPks = () => {
            const grouped = new Map<Pk, ReactionEvent[]>();
            currentNoteVotes().forEach((r: ReactionEvent) => {
              if (!grouped.has(r.pk)) {
                grouped.set(r.pk, []);
              }
              grouped.get(r.pk)!.push(r);
            });
            return [...grouped.values()]
              .map(reactionEvents => sortByDate(reactionEvents)[0]);
          };

          const getSigner = async () => {
            if (store.onLogin) {
              const acceptedLogin = await store.onLogin();
              if (!acceptedLogin) {
                return;
              }
            }
            if (!signersStore.active) {
              console.error('Error: active signer is not set!');
              return;
            }
            const signer: EventSigner = signersStore.active!;
            if (!signer?.signEvent) {
              console.error('Error: User has no signer!');
              return;
            }
            return signer;
          }

          createEffect(() => {
            batch(() => {
              const votes = currentNoteVotesDeduplicatedByPks();
              setHasVotes(votes.length > 0);

              const newVoteCount = votes
                .map(r => voteKind(r) as number)
                .reduce((sum, i) => sum + i, 0);
              setVotesCount(newVoteCount);

              const signer = signersStore.active;
              const kind: VoteKind = (signer && votes.filter(r => r.pk === signer!.pk).map(r => voteKind(r))[0]) || 0;
              setCurrentUserVote(kind);
            });
          });

          const toggleVote = async (reaction: VoteKind, note: NoteEvent) => {
            const s = await getSigner();
            if (!s) {
              return;
            }
            const signer = s!;
            const latestVote = currentUserVote();
            const newVote = latestVote === reaction ? 0 : reaction;

            const rootEventId = note.ro || store.version || store.rootEventIds[0];

            const publishVote = async () => {
              const tags = [];
              if (rootEventId) {
                tags.push(['e', rootEventId, '', 'root']);
              }

              await signAndPublishEvent({
                kind: 7,
                created_at: currentTime(),
                content: newVote === -1 ? '-' : '+',
                pubkey: signer.pk,
                tags: [
                  ...tags,
                  ['e', note.id, '', 'reply'],
                  ['p', signer.pk],
                ],
              });
            };

            const unpublishOutdatedEvents = async () => {
              const eids: Eid[] = sortByDate(currentNoteVotes().filter(r => r.pk === signer!.pk))
                .reverse()
                .map(i => i.eid);
              if (eids.length === 0) {
                return;
              }
              const sentRequest = await signAndPublishEvent({
                kind: 5,
                created_at: currentTime(),
                content: '',
                pubkey: signer.pk,
                tags: eids.map(eid => ['e', eid]),
              });
              if (sentRequest) {
                remove('reactions', eids);
              }
            };

            const signAndPublishEvent = async (unsignedEvent: UnsignedEvent) => {
              const id = getEventHash(unsignedEvent);
              const signature = await signer.signEvent!(unsignedEvent);
              const event: Event = { id, ...unsignedEvent, ...signature };
              console.log(JSON.stringify(event, null, 2));
              const [ok, failures] = await publishEvent(event, relays(), 7000);
              return ok > 0;
            }

            await unpublishOutdatedEvents();
            if ([-1, 1].includes(newVote)) {
              await publishVote();
            }
          };

          const MAX_LENGTH = 255;
          const [target, setTarget] = createSignal<HTMLElement>();

          const [profilePicture, setProfilePicture] = createSignal(defaultPicture);

          const pubkey = () => event().pk;
          const npub = () => npubEncode(pubkey());
          const profile = () => profiles().find(p => p.pk === pubkey());

          createEffect(async () => {
            setProfilePicture(profile()?.i || defaultPicture);
          });

          // Update createdAt every minute
          let timer: any;
          const createdAt = () => timeAgo(event().ts * 1000);
          const [createdTimeAgo, setCreatedTimeAgo] = createSignal<string>();

          createEffect(() => {
            setCreatedTimeAgo(createdAt());
            timer = setInterval(() => {
              setCreatedTimeAgo(createdAt());
            }, 60 * 1000);
          });

          const isAnchorMentioned = () => event().a === anchor().value && event().am;

          const action = () => event().k === 9802 ? 'highlight' : (isAnchorMentioned() ? 'mention' : 'comment');

          const total = createMemo(() => totalChildren(event()));
          const firstLevelComments = () => props.firstLevelComments && props.firstLevelComments() || 0;

          if (!event().parent) {
            setTimeout(() => {
              batch(() => {
                if (!frozenAutoThreadCollapse()) {
                  if (firstLevelComments() < MIN_AUTO_COLLAPSED_THREADS || total() < MIN_AUTO_COLLAPSED_COMMENTS) {
                    setFreezeAutoThreadCollapse(true);
                    setThreadCollapsed(false);
                  }
                }
              })
            }, AUTO_UNCOLLAPSE_THREADS_TIMEOUT_MS);

            createEffect(on([visible], () => visible() && setFreezeAutoThreadCollapse(true)));
          }

          const isUnspecifiedVersion = () =>
            // if it does not have a parent or rootId
            !event().parent && !event().ro;

          const isMissingEvent = () =>
            // if it does not have a parent
            !event().parent &&
            // does have a root but it's not in the rootEvents
            event().ro && !store.rootEventIds.includes(event().ro!);

          const isDifferentVersion = () =>
            // if it does not have a parent
            !event().parent &&
            // does have a root in root events
            event().ro && store.rootEventIds.includes(event().ro!)
            // but does not match the current version
            && store.version && store.version !== event().ro;

          onCleanup(() => clearInterval(timer));

          const parsedContent = createMemo(() => {
            const result = parseContent(event(), store, props.articles());
            if (!overflowed() && result.length > MAX_LENGTH) {
              setOverflowed(true);
            }
            return result;
          });

          return <div class="ztr-comment">
            <div class="ztr-comment-body">
              <div class="ztr-comment-info-wrapper">
                <div class="ztr-comment-info">
                  <div class="ztr-comment-info-picture">
                    <img src={profilePicture()} onerror={() => setProfilePicture(defaultPicture)} />
                  </div>
                  <ul class="ztr-comment-info-items">
                    <li class="ztr-comment-info-author">
                      <a href={store.urlPrefixes!.npub + npub()} target="_blank" >{profile()?.n || shortenEncodedId(npub())}</a>
                      <span style="white-space: nowrap;"><a href={store.urlPrefixes!.note + noteEncode(event().id)} target="_blank" style="font-weight: 400"><small>{action()}ed {createdTimeAgo()}</small></a></span>
                    </li>
                  </ul>
                </div>
              </div>

              {isMissingEvent() && <div class="ztr-comment-text"><p class="warning">{warningSvg()}<span>This is a {action()} that referenced this article in <a href={store.urlPrefixes!.note + noteEncode(event().ro!)}>another thread</a></span></p></div>}
              {isUnspecifiedVersion() && <div class="ztr-comment-text"><p class="warning">{warningSvg()}<span>Article contents may have changed since this {action()} was made</span></p></div>}
              {isDifferentVersion() && <div class="ztr-comment-text"><p class="warning">{warningSvg()}<span>Article contents changed since this {action()} was made</span></p></div>}

              <div
                ref={setTarget}
                classList={{ "ztr-comment-text": true, "ztr-comment-text-fade": overflowed() && !isExpanded(), "highlight": event().k == 9802 }}
                innerHTML={parsedContent()}>
              </div>

              {overflowed() &&
                <div class="ztr-comment-expand" onClick={() => setExpanded(!isExpanded())}>
                  <span>{isExpanded() ? 'Show less' : 'Read more'}</span>
                </div>}

              <ul class="ztr-comment-actions">
                {<Show when={!store.disableFeatures!.includes('votes')}>
                  <li class="ztr-comment-action-upvote" classList={{selected: currentUserVote() === 1}} onClick={() => toggleVote(1, event())}>
                    {currentUserVote() === 1 ? upvoteSelectedSvg() : upvoteSvg()}
                  </li>
                  <li class="ztr-comment-action-votes">
                    <span>{hasVotes() ? votesCount() : 'Vote'}</span>
                  </li>
                  <li class="ztr-comment-action-downvote" classList={{selected: currentUserVote() === -1}} onClick={() => toggleVote(-1, event())}>
                    {currentUserVote() === -1 ? downvoteSelectedSvg() : downvoteSvg()}
                  </li>
                </Show>}
                {/* <Show when={!store.disableFeatures!.includes('zaps')}>
                  <li class="ztr-comment-action-zap">
                    {lightningSvg()}
                    <span>10</span>
                  </li>
                </Show> */}
               {/* <Show when={!store.disableFeatures!.includes('likes')}>
                  <li class="ztr-comment-action-like">
                    {likeSvg()}
                    <span>27</span>
                  </li>
                </Show> */}
                <Show when={!store.disableFeatures!.includes('reply')}>
                  <li class="ztr-comment-action-reply" onClick={() => setOpen(!isOpen())}>
                    {replySvg()}
                    <span>{isOpen() ? 'Cancel' : 'Reply'}</span>
                  </li>
                </Show>
              </ul>
              {isOpen() &&
                <ReplyEditor replyTo={event().id} onDone={() => setOpen(false)} />}
            </div>

            <div class="ztr-comment-replies" ref={commentRepliesElement}>
              <div class="ztr-comment-replies-info-actions">
                {total() > 0 &&
                <>
                  <ul class="ztr-comment-replies-info-items" classList={{selected: isThreadCollapsed()}}
                    onClick={() => setThreadCollapsed(!isThreadCollapsed())}>
                    <li><span>{isThreadCollapsed() ? upArrow() : downArrow()}</span></li>
                    {isThreadCollapsed() && <li>{total()} repl{total() > 1 ? 'ies' : 'y'}</li>}
                  </ul>
                </>}
              </div>
              {!isThreadCollapsed() && <Thread nestedEvents={() => event().children} articles={props.articles} votes={props.votes} />}
            </div>
          </div>;
        }
      }
    </Index>
  </div>;
};

// SVG

const separatorSvg = () => <svg class="ztr-comment-info-separator" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16">
  <circle cx="6" cy="6" r="6" />
</svg>;

export const nostrSvg = () => <svg width="40" height="46" viewBox="0 0 525 600" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M442.711 254.675C454.209 334.323 371.764 351.177 324.332 349.703C321.286 349.608 318.416 351.091 316.617 353.55C311.71 360.258 303.867 368.932 297.812 368.932C291.012 368.932 284.892 382.009 282.101 390.146C281.808 391 282.648 391.788 283.507 391.509C358.626 367.118 388.103 358.752 400.801 361.103C411.052 363.001 430.226 397.321 438.532 414.244C417.886 415.43 411.005 396.926 406.259 393.604C402.462 390.947 401.038 403.094 400.801 409.499C396.292 407.127 390.597 403.806 390.122 393.604C389.648 383.403 384.427 386.013 380.393 386.487C376.359 386.962 314.423 406.89 305.169 409.499C295.914 412.109 285.235 415.667 276.455 421.836C261.743 428.953 252.251 423.971 249.64 411.872C247.552 402.192 261.11 372.411 268.15 358.731C259.449 361.973 241.382 368.552 238.724 368.932C236.147 369.3 203.454 394.539 185.97 408.253C184.95 409.053 184.294 410.225 184.059 411.499C181.262 426.698 172.334 430.952 156.856 438.917C144.4 445.326 100.9 512.174 78.5318 548.261C76.9851 550.756 74.7151 552.673 72.2776 554.31C58.0697 563.847 43.5996 588.121 37.7308 600C29.3778 575.897 40.262 555.162 46.7482 547.808C43.9006 545.341 37.3353 546.78 34.4086 547.808C42.4768 520.051 70.7156 520.051 69.7664 520.051C74.987 515.544 136.211 428.004 137.634 423.971C139.047 419.97 136.723 401.262 167.494 391.005C167.982 390.842 168.467 390.619 168.9 390.34C192.873 374.933 210.857 350.094 216.893 339.514C184.887 337.314 148.236 319.47 129.072 308.008C124.941 305.538 120.56 303.387 115.777 302.846C89.5384 299.876 63.2668 316.661 52.6808 326.229C48.3144 320.156 53.2345 302.506 56.2403 294.44C45.6092 292.921 29.979 308.357 23.4928 316.265C16.4687 305.258 22.6227 285.583 26.5777 277.121C12.5295 277.501 3.00581 283.922 0 287.085C11.6277 200.494 100.569 225.677 101.565 226.827C96.8187 221.893 97.2142 215.44 98.0052 212.83C151.872 214.254 177.026 205.239 192.925 195.986C318.22 126.002 372.206 168.942 392.139 179.736C412.073 190.53 448.261 194.919 473.296 184.955C503.196 171.432 498.695 136.577 492.636 123.274C485.992 108.683 448.498 88.5179 435.209 63.0154C421.921 37.5129 433.5 6.50012 448.895 2.17754C463.562 -1.94096 474.037 2.85041 482.076 10.1115C487.436 14.9524 502.484 18.4148 508.773 20.6685C515.061 22.9223 525.502 25.8877 524.909 27.667C524.316 29.4463 514.249 29.2088 512.688 29.2088C509.485 29.2088 505.688 29.6833 509.485 31.4626C513.937 33.2682 520.657 35.8924 523.875 37.6198C524.294 37.8443 524.207 38.4293 523.749 38.5526C501.912 44.4295 482.414 32.3029 467.957 46.2898C453.244 60.524 484.568 71.4369 500.704 88.5179C516.841 105.599 533.452 126.001 520.163 172.5C509.875 208.497 466.758 239.103 444.486 251.075C443.205 251.764 442.503 253.235 442.711 254.675Z" fill="#2B2B2B"/></svg>;

const replySvg = () => <svg width={svgWidth} height={svgWidth} viewBox="0 -6 60 60" xmlns="http://www.w3.org/2000/svg"><path d="M 12.6030 50.4905 C 13.3758 50.4905 13.9307 50.1140 14.8621 49.2421 L 20.6483 43.8720 C 19.5188 42.9803 18.6073 41.5733 18.6073 38.3433 L 18.6073 25.2052 C 18.6073 19.1217 22.3129 15.5152 28.3766 15.5152 L 42.2479 15.5152 L 42.2281 14.7622 C 41.9306 10.6999 39.2557 8.0643 34.7177 8.0643 L 7.5301 8.0643 C 2.9922 8.0643 0 10.7791 0 15.4954 L 0 34.9548 C 0 39.6710 2.9922 42.7028 7.5301 42.7028 L 10.8195 42.7028 L 10.8195 48.4693 C 10.8195 49.6979 11.4735 50.4905 12.6030 50.4905 Z M 44.6058 53.2450 C 45.7353 53.2450 46.3895 52.4325 46.3895 51.2237 L 46.3895 45.4374 L 48.4702 45.4374 C 53.0078 45.4374 56 42.4056 56 37.7092 L 56 25.6610 C 56 20.9250 53.0078 18.2300 48.4702 18.2300 L 28.8522 18.2300 C 24.1161 18.2300 21.3221 20.9250 21.3221 25.6610 L 21.3221 37.7092 C 21.3221 42.4056 24.1161 45.4374 28.8522 45.4374 L 35.1735 45.4374 L 42.3470 51.9767 C 43.2784 52.8487 43.8331 53.2450 44.6058 53.2450 Z" /></svg>;
export const lightningSvg = () => <svg xmlns="http://www.w3.org/2000/svg" width={svgWidth} height={svgWidth} viewBox="-120 -80 528 588"><path d="M349.4 44.6c5.9-13.7 1.5-29.7-10.6-38.5s-28.6-8-39.9 1.8l-256 224c-10 8.8-13.6 22.9-8.9 35.3S50.7 288 64 288H175.5L98.6 467.4c-5.9 13.7-1.5 29.7 10.6 38.5s28.6 8 39.9-1.8l256-224c10-8.8 13.6-22.9 8.9-35.3s-16.6-20.7-30-20.7H272.5L349.4 44.6z" /></svg>;
export const likeSvg = () => <svg width={svgWidth} height={svgWidth} viewBox="0 -16 180 180" xmlns="http://www.w3.org/2000/svg"><path d="M60.732 29.7C41.107 29.7 22 39.7 22 67.41c0 27.29 45.274 67.29 74 94.89 28.744-27.6 74-67.6 74-94.89 0-27.71-19.092-37.71-38.695-37.71C116 29.7 104.325 41.575 96 54.066 87.638 41.516 76 29.7 60.732 29.7z" /></svg>;
const upvoteSvg = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M323.8 34.8c-38.2-10.9-78.1 11.2-89 49.4l-5.7 20c-3.7 13-10.4 25-19.5 35l-51.3 56.4c-8.9 9.8-8.2 25 1.6 33.9s25 8.2 33.9-1.6l51.3-56.4c14.1-15.5 24.4-34 30.1-54.1l5.7-20c3.6-12.7 16.9-20.1 29.7-16.5s20.1 16.9 16.5 29.7l-5.7 20c-5.7 19.9-14.7 38.7-26.6 55.5c-5.2 7.3-5.8 16.9-1.7 24.9s12.3 13 21.3 13L448 224c8.8 0 16 7.2 16 16c0 6.8-4.3 12.7-10.4 15c-7.4 2.8-13 9-14.9 16.7s.1 15.8 5.3 21.7c2.5 2.8 4 6.5 4 10.6c0 7.8-5.6 14.3-13 15.7c-8.2 1.6-15.1 7.3-18 15.2s-1.6 16.7 3.6 23.3c2.1 2.7 3.4 6.1 3.4 9.9c0 6.7-4.2 12.6-10.2 14.9c-11.5 4.5-17.7 16.9-14.4 28.8c.4 1.3 .6 2.8 .6 4.3c0 8.8-7.2 16-16 16H286.5c-12.6 0-25-3.7-35.5-10.7l-61.7-41.1c-11-7.4-25.9-4.4-33.3 6.7s-4.4 25.9 6.7 33.3l61.7 41.1c18.4 12.3 40 18.8 62.1 18.8H384c34.7 0 62.9-27.6 64-62c14.6-11.7 24-29.7 24-50c0-4.5-.5-8.8-1.3-13c15.4-11.7 25.3-30.2 25.3-51c0-6.5-1-12.8-2.8-18.7C504.8 273.7 512 257.7 512 240c0-35.3-28.6-64-64-64l-92.3 0c4.7-10.4 8.7-21.2 11.8-32.2l5.7-20c10.9-38.2-11.2-78.1-49.4-89zM32 192c-17.7 0-32 14.3-32 32V448c0 17.7 14.3 32 32 32H96c17.7 0 32-14.3 32-32V224c0-17.7-14.3-32-32-32H32z"/></svg>;
const downvoteSvg = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M323.8 477.2c-38.2 10.9-78.1-11.2-89-49.4l-5.7-20c-3.7-13-10.4-25-19.5-35l-51.3-56.4c-8.9-9.8-8.2-25 1.6-33.9s25-8.2 33.9 1.6l51.3 56.4c14.1 15.5 24.4 34 30.1 54.1l5.7 20c3.6 12.7 16.9 20.1 29.7 16.5s20.1-16.9 16.5-29.7l-5.7-20c-5.7-19.9-14.7-38.7-26.6-55.5c-5.2-7.3-5.8-16.9-1.7-24.9s12.3-13 21.3-13L448 288c8.8 0 16-7.2 16-16c0-6.8-4.3-12.7-10.4-15c-7.4-2.8-13-9-14.9-16.7s.1-15.8 5.3-21.7c2.5-2.8 4-6.5 4-10.6c0-7.8-5.6-14.3-13-15.7c-8.2-1.6-15.1-7.3-18-15.2s-1.6-16.7 3.6-23.3c2.1-2.7 3.4-6.1 3.4-9.9c0-6.7-4.2-12.6-10.2-14.9c-11.5-4.5-17.7-16.9-14.4-28.8c.4-1.3 .6-2.8 .6-4.3c0-8.8-7.2-16-16-16H286.5c-12.6 0-25 3.7-35.5 10.7l-61.7 41.1c-11 7.4-25.9 4.4-33.3-6.7s-4.4-25.9 6.7-33.3l61.7-41.1c18.4-12.3 40-18.8 62.1-18.8H384c34.7 0 62.9 27.6 64 62c14.6 11.7 24 29.7 24 50c0 4.5-.5 8.8-1.3 13c15.4 11.7 25.3 30.2 25.3 51c0 6.5-1 12.8-2.8 18.7C504.8 238.3 512 254.3 512 272c0 35.3-28.6 64-64 64l-92.3 0c4.7 10.4 8.7 21.2 11.8 32.2l5.7 20c10.9 38.2-11.2 78.1-49.4 89zM32 384c-17.7 0-32-14.3-32-32V128c0-17.7 14.3-32 32-32H96c17.7 0 32 14.3 32 32V352c0 17.7-14.3 32-32 32H32z"/></svg>;
const upvoteSelectedSvg = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M313.4 32.9c26 5.2 42.9 30.5 37.7 56.5l-2.3 11.4c-5.3 26.7-15.1 52.1-28.8 75.2H464c26.5 0 48 21.5 48 48c0 18.5-10.5 34.6-25.9 42.6C497 275.4 504 288.9 504 304c0 23.4-16.8 42.9-38.9 47.1c4.4 7.3 6.9 15.8 6.9 24.9c0 21.3-13.9 39.4-33.1 45.6c.7 3.3 1.1 6.8 1.1 10.4c0 26.5-21.5 48-48 48H294.5c-19 0-37.5-5.6-53.3-16.1l-38.5-25.7C176 420.4 160 390.4 160 358.3V320 272 247.1c0-29.2 13.3-56.7 36-75l7.4-5.9c26.5-21.2 44.6-51 51.2-84.2l2.3-11.4c5.2-26 30.5-42.9 56.5-37.7zM32 192H96c17.7 0 32 14.3 32 32V448c0 17.7-14.3 32-32 32H32c-17.7 0-32-14.3-32-32V224c0-17.7 14.3-32 32-32z"/></svg>;
const downvoteSelectedSvg = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M313.4 479.1c26-5.2 42.9-30.5 37.7-56.5l-2.3-11.4c-5.3-26.7-15.1-52.1-28.8-75.2H464c26.5 0 48-21.5 48-48c0-18.5-10.5-34.6-25.9-42.6C497 236.6 504 223.1 504 208c0-23.4-16.8-42.9-38.9-47.1c4.4-7.3 6.9-15.8 6.9-24.9c0-21.3-13.9-39.4-33.1-45.6c.7-3.3 1.1-6.8 1.1-10.4c0-26.5-21.5-48-48-48H294.5c-19 0-37.5 5.6-53.3 16.1L202.7 73.8C176 91.6 160 121.6 160 153.7V192v48 24.9c0 29.2 13.3 56.7 36 75l7.4 5.9c26.5 21.2 44.6 51 51.2 84.2l2.3 11.4c5.2 26 30.5 42.9 56.5 37.7zM32 384H96c17.7 0 32-14.3 32-32V128c0-17.7-14.3-32-32-32H32C14.3 96 0 110.3 0 128V352c0 17.7 14.3 32 32 32z"/></svg>;

export const ellipsisSvg = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 -200 560 640"><path d="M8 256a56 56 0 1 1 112 0A56 56 0 1 1 8 256zm160 0a56 56 0 1 1 112 0 56 56 0 1 1 -112 0zm216-56a56 56 0 1 1 0 112 56 56 0 1 1 0-112z" /></svg>;
const upArrow = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512"><path d="M201.4 374.6c12.5 12.5 32.8 12.5 45.3 0l160-160c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L224 306.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l160 160z"/></svg>;
const downArrow = () => <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512"><path d="M201.4 137.4c12.5-12.5 32.8-12.5 45.3 0l160 160c12.5 12.5 12.5 32.8 0 45.3s-32.8 12.5-45.3 0L224 205.3 86.6 342.6c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3l160-160z"/></svg>;

const warningSvg = () => <svg xmlns="http://www.w3.org/2000/svg" height={svgWidth} viewBox="0 0 512 512"><path d="M256 32c14.2 0 27.3 7.5 34.5 19.8l216 368c7.3 12.4 7.3 27.7 .2 40.1S486.3 480 472 480H40c-14.3 0-27.6-7.7-34.7-20.1s-7-27.8 .2-40.1l216-368C228.7 39.5 241.8 32 256 32zm0 128c-13.3 0-24 10.7-24 24V296c0 13.3 10.7 24 24 24s24-10.7 24-24V184c0-13.3-10.7-24-24-24zm32 224a32 32 0 1 0 -64 0 32 32 0 1 0 64 0z" /></svg>;
