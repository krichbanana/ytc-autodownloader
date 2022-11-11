#!/usr/bin/env bash
channelbase="$1"
channelregex="/channel/([^/]+)"
if [[ "$channelbase" =~ $channelregex ]]; then
    channelbase="${BASH_REMATCH[1]}"
fi

if [[ -z "$channelbase" ]]; then
    echo '(channel live endpoint scraper) channel id is empty' >&2
    exit 1
fi

suf_live="/live"

tmppre="tmp.${channelbase?}"
rm "${tmppre}.url" 2>/dev/null

readonly ytdlp_cmd="../yt-dlp/yt-dlp.sh"

# Disable cookied live endpoint scrapes
# [[ -f cookies_ok.sh ]] && . cookies_ok.sh || cookies_ok() { return 0; }
cookies_ok() { return 1; } # exit 1  = false
if ! cookies_ok; then
    cookies_ok=0
else
    cookies_ok=1
fi

if [[ "$cookies_ok" == 1 ]]; then
    if [[ ! -f "cookies/${channelbase}.cookies" ]] && [[ -f "cookies/${channelbase}.txt" ]]; then
        cp cookies/${channelbase}.{txt,cookies} -av
    fi
    if test -f "cookies/${channelbase}.cookies"; then
        has_cookies=1
        cookie_file="${channelbase}.cookies"
    fi
fi

next_scrape_file="${channelbase}.live.next_scrape"
curr_time="$(date "+%s")"  # epoch time (seconds)
if [[ -f "${next_scrape_file?}" ]]; then
    saved_next_time="$(<"${next_scrape_file?}")"
    if ((saved_next_time > curr_time)); then
        # throttled
        echo 'throttled...'
        exit 0
    fi
fi

next_time=$((curr_time + (60*2)))  # 2 min throttle time
echo "$next_time" >"$next_scrape_file"

touch "channel-cached/${channelbase}.meta.live.new"
touch "channel-cached/${channelbase}.url.live.all"

# Note that premieres will only show up here.
url="https://www.youtube.com/channel/$channelbase$suf_live"
if ((has_cookies)); then
    echo 'cookies used!'
    "$ytdlp_cmd" -s -q -j --cookies="$cookie_file" --sleep-requests 0.1 --ignore-no-formats-error --use-extractors default,-generic --download-archive "channel-cached/${channelbase}.url.live.all" -- "$url" >"${tmppre}.live"
else
    echo 'cookies not used!'
    "$ytdlp_cmd" -s -q -j --sleep-requests 0.1 --ignore-no-formats-error --use-extractors default,-generic --download-archive "channel-cached/${channelbase}.url.live.all" -- "$url" >"${tmppre}.live"
fi
ecode=$?
if [[ "$ecode" != 0 ]]; then
    echo "(channel live endpoint scraper) warning: fetch for ${tmppre}.live exited with error: $ecode" >&2
fi
#jq -r <"${tmppre}.live" .url > "${tmppre}.live.url"
jq -c 'select(.id? != null)' <"${tmppre}.live" > "channel-cached/${channelbase}.meta.live.new"

# Bail out early if out actions are futile to reduce console spam.
if [[ ! -f "${tmppre}.live.url" ]]; then
    echo "(channel live endpoint scraper) there doesn't seem to be any videos on the live endpoint, aborting."
    for suffix in .live {.live,.final}.url; do
        rm "${tmppre}${suffix}" 2>/dev/null
    done
    exit 1
fi


mkdir -p channel-cached
touch "channel-cached/${channelbase}.meta.live.new"
jq -c < "channel-cached/${channelbase}.meta.live.new" > "${tmppre}.meta.live.new.tmp"
vidid="$(jq -r .id? < "${tmppre}.meta.live.new.tmp")"
mv "${tmppre}.meta.live.new.tmp" "channel-cached/${channelbase}.meta.live.new"
vid="youtube ${vidid:-null}"
if [[ "$vid" == 'youtube null' ]]; then
    exit 0
fi
echo "$vid" > "channel-cached/${channelbase}.url.live.all"
# Avoid reading live endpoint results into scraper_oo
sort "channel-cached/${channelbase}.url.all" "channel-cached/${channelbase}.url.live.all" | uniq | grep ^youtube > "channel-cached/${channelbase}.url.live.all.tmp"
mv "channel-cached/${channelbase}.url.live.all.tmp" "channel-cached/${channelbase}.url.live.all"

#echo '--- after url finagling ---'

# oldcnt="$(wc -l "channel-cached/${channelbase}.url.live.all" | cut -d ' ' -f 1)"
# # 20 limit to prevent processing way too many videos (beware of pointless m3u8 requests)
# if test -s "${tmppre}.live.url"; then
#     "$ytdlp_cmd" -s -q -j --ignore-no-formats-error --force-write-archive --download-archive "channel-cached/${channelbase}.url.live.all" --max-downloads 20 -a - <"${tmppre}.live.url" > "channel-cached/${channelbase}.meta.live.new"
# else
#     echo "no urls... cookies_ok=${cookies_ok}"
# fi
# newcnt="$(wc -l "channel-cached/${channelbase}.url.live.all" | cut -d ' ' -f 1)"
# echo "(channel live endpoint scraper)" "${newcnt?} (+$((newcnt - oldcnt))) entries now in channel-cached/${channelbase}.url.live.all"
# metacnt="$(wc -l "channel-cached/${channelbase}.meta.live.new" | cut -d ' ' -f 1)"
# if ((metacnt > 0)); then
#     echo "(channel live endpoint scraper)" "${metacnt?} entries now in channel-cached/${channelbase}.meta.live.new"
# fi


# if URL is in url.all but not in meta.new, the scraper will directly scrape the metainfo itself. Else, meta.new is used.


