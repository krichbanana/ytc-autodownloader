#!/usr/bin/env bash
channelbase="$1"
channelregex="/channel/([^/]+)"
if [[ "$channelbase" =~ $channelregex ]]; then
    channelbase="${BASH_REMATCH[1]}"
fi

suf_community="/community"

tmppre="tmp.${channelbase?}"
rm "${tmppre}.url" 2>/dev/null

readonly ytdlp_cmd="../yt-dlp/yt-dlp.sh"

if test -f "${channelbase}.cookies"; then
    has_cookies=1
    cookie_file="${channelbase}.cookies"
fi

# Note that premieres will only show up here.
url="https://www.youtube.com/channel/$channelbase$suf_community"
if ((has_cookies)); then
    "$ytdlp_cmd" -s -q -j --cookies="$cookie_file" --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.community"
else
    "$ytdlp_cmd" -s -q -j --sleep-requests 0.1 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.community"
fi
ecode=$?
if [[ "$ecode" != 0 ]]; then
    echo "(channel tab scraper) warning: fetch for ${tmppre}.community exited with error: $ecode" >&2
fi
jq -r <"${tmppre}.community" .url > "${tmppre}.community.url"

# Bail out early if out actions are futile to reduce console spam.
if [[ ! -f "${tmppre}.community.url" ]]; then
    echo "(channel tab scraper) there doesn't seem to be any videos on the community tab, aborting."
    for suffix in .community {.community,.final}.url; do
        rm "${tmppre}${suffix}" 2>/dev/null
    done
    exit 1
fi

# Create an onmilist of urls (with possible duplicates)
mv "${tmppre}.community.url" "${tmppre}.final.url"

mkdir -p channel-cached
touch "channel-cached/${channelbase}.url.all"

oldcnt="$(wc -l "channel-cached/${channelbase}.url.all" | cut -d ' ' -f 1)"
# 20 limit to prevent processing way too many videos (beware of pointless m3u8 requests)
"$ytdlp_cmd" -s -q -j --ignore-no-formats-error --force-write-archive --download-archive "channel-cached/${channelbase}.url.all" --max-downloads 20 -a - <"${tmppre}.final.url" > "channel-cached/${channelbase}.meta.new"
newcnt="$(wc -l "channel-cached/${channelbase}.url.all" | cut -d ' ' -f 1)"
echo "(channel tab scraper)" "${newcnt?} (+$((newcnt - oldcnt))) entries now in channel-cached/${channelbase}.url.all"
metacnt="$(wc -l "channel-cached/${channelbase}.meta.new" | cut -d ' ' -f 1)"
if ((metacnt > 0)); then
    echo "(channel tab scraper)" "${metacnt?} entries now in channel-cached/${channelbase}.meta.new"
fi

for suffix in .community .final.url; do
    rm "${tmppre}${suffix}"
done

# if URL is in url.all but not in meta.new, the scraper will directly scrape the metainfo itself. Else, meta.new is used.


