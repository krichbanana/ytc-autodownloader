#!/usr/bin/env bash
channelbase="$1"
channelregex="/channel/([^/]+)"
if [[ "$channelbase" =~ $channelregex ]]; then
    channelbase="${BASH_REMATCH[1]}"
fi

suf_featured="/featured"
suf_streams="/streams"
key_featured="featured"
key_streams="streams"
key_uploads="uploads"

tmppre="tmp.${channelbase?}"
rm "${tmppre}.url" 2>/dev/null

readonly ytdlp_cmd="../yt-dlp/yt-dlp.sh"


download_video_list() {
    suffix="${1?}"
    key="${2:?}"
    url="${3:-https://www.youtube.com/channel/"$channelbase$suffix"}"
    # 100 limit to avoid too many rather useless paginations
    "$ytdlp_cmd" -s -q -j --sleep-requests 0.01 --ignore-no-formats-error --flat-playlist --playlist-end 50 -- "$url" >"${tmppre}.$key"
    ecode=$?
    if [[ "$ecode" != 0 ]]; then
        echo "(channel scraper) warning: fetch for ${tmppre}.$key exited with error: $ecode" >&2
    fi
    jq -r <"${tmppre}.$key" 'select(.id != null)|.id' > "${tmppre}.$key.url"
    cat "${tmppre}.$key.url" >>"${tmppre}.url"
}


# Note that upcoming premieres will only show up here, probably.
download_video_list $suf_featured $key_featured
# Should be the most responsive source; upcoming streams/premieres should show here too.
download_video_list '' $key_uploads "https://www.youtube.com/playlist?list=UU${channelbase:2}"
#download_video_list $suf_streams $key_streams

# Create an onmilist of urls (with possible duplicates)
cp "${tmppre}.url" "${tmppre}.final.url"

if [[ ! -s "${tmppre}.final.url" ]]; then
    echo "error: no urls for yt-dlp" >&2
    exit 1
fi

mkdir -p channel-cached
touch "channel-cached/${channelbase}.url.all"

oldcnt="$(wc -l "channel-cached/${channelbase}.url.all" | cut -d ' ' -f 1)"
# a limit is set to prevent processing way too many videos (beware of pointless m3u8 requests)
: > "channel-cached/${channelbase}.meta.new"
for key in "${key_featured?}" "${key_uploads?}" "final"; do
    if [[ -s "${tmppre}.${key}.url" ]]; then
        "$ytdlp_cmd" -s -q -j --ignore-no-formats-error --force-write-archive --download-archive "channel-cached/${channelbase}.url.all" --max-downloads 3 -a - <"${tmppre}.${key}.url" >> "channel-cached/${channelbase}.meta.new"
    fi
done
newcnt="$(wc -l "channel-cached/${channelbase}.url.all" | cut -d ' ' -f 1)"
echo "(channel scraper)" "${newcnt?} (+$((newcnt - oldcnt))) entries now in channel-cached/${channelbase}.url.all"
metacnt="$(wc -l "channel-cached/${channelbase}.meta.new" | cut -d ' ' -f 1)"
if ((metacnt > 0)); then
    echo "(channel scraper)" "${metacnt?} entries now in channel-cached/${channelbase}.meta.new"
fi

# if URL is in url.all but not in meta.new, the scraper will directly scrape the metainfo itself. Else, meta.new is used.


