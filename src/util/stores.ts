import { ReactiveSet } from "@solid-primitives/set";
import { Event } from "nostr-tools/core";
import { UnsignedEvent } from "nostr-tools/pure";
import { SimplePool } from "nostr-tools/pool";
import { Filter } from "nostr-tools/filter";
import { WindowNostr } from "nostr-tools/nip07";
import { Profile } from "./models.ts";
import { createMutable } from "solid-js/store";
import { Eid } from "./models.ts";
import { NestedNoteEvent } from "./nest.ts";

// Global data (for now)
export const pool = new SimplePool();

export const store = createMutable<PreferencesStore>({
  readRelays: [],
  writeRelays: [],
  rootEventIds: [],
  topRootEventIds: new Set,
  userObservedComments: false,
  userStartedReadingComments: false,
  threadCollapsed: new Map,
  messageExpanded: new ReactiveSet,
  languages: [],
  maxCommentLength: 0,
  validatedEvents: new Map,
  validateReadPow: true,
  writePowDifficulty: 0,
  filter: {},
  profiles: () => [],
});

export const signersStore = createMutable<SignersStore>({});

// Signing

export type SignersStore = {
  [key in 'active' | 'anonymous' | 'internal' | 'external']?: EventSigner;
};
export type SignEvent = (event: UnsignedEvent) => Promise<{ sig: string; }>;
export type EventSigner = {
  pk: string,
  signEvent?: SignEvent;
};

export type UrlPrefixesKeys = 'naddr' | 'nevent' | 'note' | 'npub' | 'nprofile' | 'tag';

const _types = ['reply', 'likes', 'votes', 'zaps', 'publish', 'watch', 'replyAnonymously', 'hideContent'] as const;
type DisableType = typeof _types[number];
export const isDisableType = (type: string): type is DisableType => {
  return _types.includes(type as DisableType);
};

export type PreferencesStore = {
  anchor?: Anchor, // derived from anchor prop
  readRelays: string[];
  writeRelays: string[];
  version?: string;  // derived from version prop
  rootEventIds: string[];  // derived from anchor prop
  topRootEventIds: Set<Eid>,
  userObservedComments: boolean,
  userStartedReadingComments: boolean,
  threadCollapsed: Map<Eid, boolean>,
  messageExpanded: ReactiveSet<Eid>,
  languages: string[],
  maxCommentLength: number,
  validatedEvents: Map<Eid, boolean>,
  validateReadPow: boolean,
  writePowDifficulty: number;
  filter: Filter;  // derived from anchor prop
  externalAuthor?: string; // prop, mostly used with http anchor type
  disableFeatures?: DisableType[]; // prop
  urlPrefixes?: { [key in UrlPrefixesKeys]?: string }, // prop
  replyPlaceholder?: string,

  anchorAuthor?: string;
  profiles: () => Profile[];
  onLogin?: () => Promise<boolean>;
  onPublish?: (id: Eid, kind: number, content: string) => Promise<boolean>;
  onReceive?: (id: Eid, kind: number, content: string) => Promise<boolean>;
};

export type Anchor = { type: 'http' | 'naddr' | 'note' | 'error', value: string; };

// Globals

declare global {
  interface Window {
    nostr?: WindowNostr & {
      signEvent: SignEvent;
    };
  }
}
