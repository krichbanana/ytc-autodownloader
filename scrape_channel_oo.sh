#!/usr/bin/env bash
channelbase="$1"
channelregex="/channel/([^/]+)"
if [[ "$channelbase" =~ $channelregex ]]; then
    channelbase="${BASH_REMATCH[1]}"
fi

suf_futurelives="/videos?view=2&live_view=501"
suf_currentlives="/videos?view=2&live_view=502"
suf_pastlives="/videos?view=2&live_view=503"
suf_allvideos="/videos?view=57"

tmppre="tmp.${channelbase?}"
rm "${tmppre}.url" 2>/dev/null

readonly ytdlp_cmd="../yt-dlp/yt-dlp.sh"

url="https://www.youtube.com/channel/$channelbase$suf_futurelives"
"$ytdlp_cmd" -s -q -j --sleep-requests 0.5 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.501"
ecode=$?
if [[ "$ecode" != 0 ]]; then
    echo "(channel scraper) warning: fetch for ${tmppre}.501 exited with error: $ecode" >&2
fi
# Extract url fields from a flat playlist
jq -r <"${tmppre}.501" 'select(.id != null)|.id' > "${tmppre}.501.url"
# 5 is arbitrary threshhold
if [[ "$(wc -l "${tmppre}.501.url" | cut -d ' ' -f 1)" -lt 5 ]]; then
    # likely not redirected
    cat "${tmppre}.501.url" >>"${tmppre}.url"
fi

url="https://www.youtube.com/channel/$channelbase$suf_currentlives"
"$ytdlp_cmd" -s -q -j --sleep-requests 0.5 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.502"
ecode=$?
if [[ "$ecode" != 0 ]]; then
    echo "(channel scraper) warning: fetch for ${tmppre}.502 exited with error: $ecode" >&2
fi
jq -r <"${tmppre}.502" 'select(.id != null)|.id' > "${tmppre}.502.url"
if [[ "$(wc -l "${tmppre}.502.url" | cut -d ' ' -f 1)" -lt 5 ]]; then
    # likely not redirected
    cat "${tmppre}.502.url" >>"${tmppre}.url"
fi

# Note that premieres will only show up here.
url="https://www.youtube.com/channel/$channelbase$suf_allvideos"
"$ytdlp_cmd" -s -q -j --sleep-requests 0.5 --ignore-no-formats-error --flat-playlist "$url" >"${tmppre}.57"
ecode=$?
if [[ "$ecode" != 0 ]]; then
    echo "(channel scraper) warning: fetch for ${tmppre}.57 exited with error: $ecode" >&2
fi
jq -r <"${tmppre}.57" 'select(.id != null)|.id' > "${tmppre}.57.url"

if [[ ! -f "${tmppre}.url" ]]; then
    echo "(channel scraper) there doesn't seem to be any live or upcoming livestreams"
    touch "${tmppre}.url"
fi

# Create an onmilist of urls (with possible duplicates)
cat "${tmppre}.url" "${tmppre}.57.url" >"${tmppre}.final.url"

if [[ ! -s "${tmppre}.final.url" ]]; then
    echo "error: no urls for yt-dlp" >&2
    exit 1
fi

mkdir -p channel-cached
touch "channel-cached/${channelbase}.url.all"

oldcnt="$(wc -l "channel-cached/${channelbase}.url.all" | cut -d ' ' -f 1)"
# 20 limit to prevent processing way too many videos (beware of pointless m3u8 requests)
"$ytdlp_cmd" -s -q -j --ignore-no-formats-error --force-write-archive --download-archive "channel-cached/${channelbase}.url.all" --max-downloads 20 -a - <"${tmppre}.final.url" > "channel-cached/${channelbase}.meta.new"
newcnt="$(wc -l "channel-cached/${channelbase}.url.all" | cut -d ' ' -f 1)"
echo "(channel scraper)" "${newcnt?} (+$((newcnt - oldcnt))) entries now in channel-cached/${channelbase}.url.all"
metacnt="$(wc -l "channel-cached/${channelbase}.meta.new" | cut -d ' ' -f 1)"
if ((metacnt > 0)); then
    echo "(channel scraper)" "${metacnt?} entries now in channel-cached/${channelbase}.meta.new"
fi

for suffix in .{501,502,57}{,.url} {.final,}.url; do
    rm "${tmppre}${suffix}"
done

# if URL is in url.all but not in meta.new, the scraper will directly scrape the metainfo itself. Else, meta.new is used.


