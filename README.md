# ytc-autodownloader
A YouTube livestream scraper and chat downloader.

## Overview
Currently, the program periodically scrapes the hololive schedule and channels.txt-listed channels for Youtube links and runs a live-chat downloader on all live and upcoming streams. The goal is for a long-running program that saves the live chat of Youtube streams, hoping to capture chat messages that would be lost from prechat/postchat or would be lost from the chat replay due to issues with Youtube.

Some other details:
- Chat downloader will retry until live chat is available.
- Chat downloader will be invoked for premieres as well as regular livestreams; uploads are ignored.
- Chat downloader will be used for more reliable metainfo and link scraping by default.
- Chat downloader will use cookies found for a channel or video if the live chat is missing.
- Runtime state is saved locally to speed up program restarts.
- Scheduled, start and end times for livestreams and live chats is recorded if feasible.
- Chat files (json and stdout) are saved locally and are compressed to substantially reduce disk usage.
- No streams are ever downloaded, only metadata and chat.

## Dependencies
- xenova's chat\_downloader, for actually downloading chat and for faster scraping than yt-dlp
- Beautiful Soup 4 (to scrape the hololive schedule)
- wget (to download the schedule)
- jq (for certain instances of JSON processing)
- lzip or zstd ("optional", to compress chat logs)
- yt-dlp (included, updated version exports metadata and to allow upcoming streams to be scraped)
- A Unix-compatible system (e.g. bash, coreutils; Cygwin has been tested but no assurances are made)

## Bugs
- Poor testing. Components too tightly coupled.
- Verbose output
- Cached metadata is not cleared until a restart.
- Restarting is slow and cumbersome and will likely rescrape all upcoming/live livestreams.
- Scraping is slow; a private chat\_downloader method can be used to get a faster scrape
- Scraping channels for links is slow; chat\_downloader is used for faster scrapes
- Scraping metainfo is slow as meta scrapes are intentionally not parallel to avoid YouTube blocks
- Compressing chat logs is intentionally not parallel to avoid CPU/memory spikes from high-strength compression.
- To avoid deadlocks, each download task causes python to fork, which costs quite a bit of memory.
- Incompatible with Windows/NT (Cygwin appears to work)
- Streams that have failed will not be reattempted and might not be reattempted after a program restart.
- Chat downloader does not guarantee that every message will be retrieved (possible in cases of high message rate or internet issues)

## Future
- Additional functionality, like saving metadata changes (implementation incomplete/untested)
- More robust scaper and downloader functionality
- Use yt-dlp's new live chat download feature as an alternative to chat\_downloader
