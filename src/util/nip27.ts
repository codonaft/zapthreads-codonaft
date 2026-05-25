// Minimal NIP-27 reference matcher/replacer.
//
// nostr-tools removed the `matchAll` / `replaceAll` helpers from its `nip27`
// module — current versions export only `parse`. ZapThreads still needs the
// old behaviour (find `nostr:`-prefixed NIP-19 references in text, then decode
// or replace them), so the small amount it used is vendored here.
//
// Behaviour mirrors nostr-tools' pre-removal `nip27`: it matches NIP-21
// `nostr:` URIs only — bare entities are normalised to `nostr:` beforehand
// in `ui.ts` (see BAD_NIP27_REGEX). Decode failures are skipped rather than
// thrown, so a malformed reference can never break comment rendering.

import { decode } from "nostr-tools/nip19";

export type Nip27Match = {
  uri: string;                        // full match, e.g. "nostr:npub1..."
  value: string;                      // the bech32 entity, e.g. "npub1..."
  decoded: ReturnType<typeof decode>; // nip19 decode result
};

// NIP-21 `nostr:` URI wrapping a bech32 entity. The bech32 body pattern
// mirrors nostr-tools' own BECH32_REGEX.
const BECH32 = /[\x21-\x7E]{1,83}1[023456789acdefghjklmnpqrstuvwxyz]{6,}/;

// A fresh RegExp per call — global regexes carry `lastIndex` state.
const nostrUriRegex = () => new RegExp(`\\bnostr:(${BECH32.source})\\b`, "g");

export function* matchAll(content: string): Iterable<Nip27Match> {
  for (const match of content.matchAll(nostrUriRegex())) {
    try {
      const [uri, value] = match;
      yield { uri, value, decoded: decode(value) };
    } catch (_e) {
      // not a valid NIP-19 entity — skip
    }
  }
}

export function replaceAll(
  content: string,
  replacer: (match: Nip27Match) => string,
): string {
  return content.replace(nostrUriRegex(), (uri, value) => {
    try {
      return replacer({ uri, value, decoded: decode(value) });
    } catch (_e) {
      return uri;
    }
  });
}
