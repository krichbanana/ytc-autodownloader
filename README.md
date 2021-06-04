# ytc-autodownloader
A really lazy livestream scraper and chat downloader.

## Overview
Currently, the program periodically scrapes the hololive schedule for Youtube links and runs a live-chat downloader on all live and upcoming streams. The goal is for a long-running program that saves the live chat of Youtube streams, hoping to capture chat messages that would be lost from prechat/postchat or would be lost from the chat replay due to issues with Youtube.

Some other details:
- Chat downloader will retry until live chat is available.
- Chat downloader will be invoked for premieres as well as regular livestreams; uploads are ignored.
- Runtime state is saved locally to speed up program restarts.
- Chat files (json and stdout) are saved locally and are compressed to reduce disk usage.
- No streams are ever downloaded, only chat.

## Dependencies
- https://github.com/xenova/chat-downloader/tree/docs (to actually download chat; the docs branch has a patch to ensure captured stdout is not block-buffered)
- Beautiful Soup 4 (to scrape the schedule)
- wget (to download the schedule)
- lzip or zstd ("optional", to compress chat logs)
- yt-dlp (included, patched to expose more metadata and to allow upcoming streams to be scraped)
- A Unix-compatible system (tee and bash are required; no effort has been made for Windows/Mac compatibility)

## Bugs
- Very poorly tested, a bunch of dead code
- Verbose, cluttered output that can't be filtered.
- Runtime errors are poorly handled.
- Stream state is never cleared.
- Restarting is cumbersome and will rescrape all upcoming/live livestreams.
- Scraping is sloow (this might help avoid throttling, though).
- To avoid deadlocks, each download task causes python to fork, which costs quite a bit of memory.
- PID tracking is completely broken (and not portable).
- Streams that have failed will not be reattempted and might not be reattempted after a program restart.
- No checking is performed on the chat downloader to ensure it downloaded the entire chat.

## Future
- Allow scraping streams from channels or from other sites.
- Additional functionality, like saving metadata changes.
- More robust scaper and downloader functionality
