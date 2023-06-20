# ZapThreads

A threaded web commenting system built on Nostr. Inspired by [stacker.news](https://stacker.news) and [NoComment](https://github.com/fiatjaf/nocomment).

![](https://nostr.build/i/0c9c2fbd41a9f6a8b0095bfbbae7562c8ed316f8cc5188de044fb453dbd2b1f5.jpg)

_(Zaps and likes count are fake random numbers at the moment)_

## Features (and goals)

Lightweight and extremely customizable. Available as web component and embeddable script.

 - [x] Threaded comments
   - [x] naddr
   - [x] URL
 - [x] Comment author metadata
 - [x] NIP-07 login
   - [ ] [Share NIP-07 session with host](https://github.com/fr4nzap/zapthreads/issues/2)
 - [x] Add comments to anchor and reply to other comments
   - [ ] [Publish and sync with relays](https://github.com/fr4nzap/zapthreads/issues/3)
   - [ ] Creation of base anchors if none
 - [x] Rich text support
   - [x] Markdown
   - [ ] Parse nostr links and references, optional image loading
 - [ ] Zaps and likes (for both naddr/anchor and comments)
   - [ ] [Ability to disable](https://github.com/fr4nzap/zapthreads/issues/4)
   - [ ] Read (NIP-45?)
   - [ ] Write
   - [ ] Splits
 - [ ] Sort by top, replies, zaps, oldest
 - [ ] Proper relay selection (NIP-05, nprofile, NIP-65)
 - [x] CSS themes (and dark theme)
   - [ ] Autodetect color mode
 - [ ] i18n, language support
   - [ ] Autodetect
   - [ ] Inherit from host
 - [ ] Optimized build
   - [ ] Reuse host NDK
   - [ ] Vite tree-shaking
 - [x] Allow to customize most elements
   - [x] Full CSS control via `shadowRoot` style
   - [ ] Better/more props (color mode, language)
 - [ ] Available as multiple libraries
   - [x] Web component (custom element)
   - [x] Embeddable script
   - [x] Solid
   - [ ] [React](https://github.com/fr4nzap/zapthreads/issues/1)
   - [ ] Vue
   - [ ] Svelte

Right now it is ~90kb gzipped with all styles and dependencies and no tree-shaking. It will get much better.

## Usage

`npm add zapthreads` (SOON™️)

```js
import "zapthreads";
// or
<script src="path/to/zapthreads.umd.cjs"></script>
<link rel="stylesheet" href="/path/to/style.css">

// ...

<zap-threads relays="wss://relay.damus.io,wss://eden.nostr.land" anchor="naddr..." />
```

Arguments:

 - `relays`: comma separated list of preferred relays
 - `anchor`: NIP-19 naddr or URL from where to retrieve anchor events

## Customize

### CSS

```js
const style = document.createElement('style');
style.innerHTML = '#ctr-root { font-size: 12em; }';
document.querySelector('zap-threads').shadowRoot.appendChild(style);
```

## Development

 - Install with `pnpm i` and run the app with `pnpm dev`
 - Build with `pnpm build`, it will place the bundles in `dist`

Any questions or ideas, please open an issue!

## LICENSE

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <http://unlicense.org/>