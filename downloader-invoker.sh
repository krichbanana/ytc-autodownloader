#!/usr/bin/env bash

# Apr 28, 2021

privatevideo_maxretries=720         # minutes in 12 hours
membersonlyvideo_maxretries=720     # minutes in 12 hours
other_maxretries=1440               # minutes in 1 day
chatlessvideo_maxretries=10080      # minutes in 1 week

# Time until next scraper call (NYI)
nextcheck=0

compress_lzip() {
    local filename="${1?}"
    lzip -9 "${filename?}"
}

compress_zstd() {
    local filename="${1?}"
    zstd -f -19 --rm "${filename?}"
}

compress() {
    compress_lzip "$1" || compress_zstd "$1" || echo "(downloader) compression failed" >&2
}

run_chat_downloader() {
    local outname="${1?}";
    local vid="${2?}";
    local pid=$$
    { chat_downloader -o "${outname?}".json --message_groups "all" --indent 2 -- "${vid?}" > "${outname?}".stdout; echo $? > exval."$pid"; } 2>&1 | tee "${outname?}.stderr"
    local exval="$(<"exval.$pid")"
    if [[ "$exval" != 0 ]]; then
        echo "(downloader) chat_downloader exited with exit status $exval" >&2
    fi
    rm "exval.$pid"
}

readonly EXIT_TRUE=0
readonly EXIT_FALSE=1
readonly EXIT_BADARG=2

certify_finished() {
    local vid="${1?}";
    local code="$(./yt-dlp/yt-dlp.sh -q -s --ignore-no-formats-error --print '%(is_live|0)d%(is_upcoming|0)d' -- "${vid?}")"
    if [[ "$code" == "00" ]]; then
        return $EXIT_TRUE
    else
        echo "(downloader) certify_finished says we aren't finished! L/U code=$code" >&2
        return $EXIT_FALSE
    fi
}

check_status() {
    local outname="${1:?}";
    local vid="${2:?}";
    test -s "${outname?}.stdout"
    if [[ $? == 0 ]]; then
        certify_finished "${vid?}"
        if [[ $? == 0 ]]; then
            return $EXIT_TRUE
        fi
    fi

    return $EXIT_FALSE
}

write_status() {
    local status="${1:?}"
    local vid="${2:?}"
    echo "${status?}" > "by-video-id/${vid?}.status"
}

run_chat_downloader_waiting() {
    local outname="${1:?}";
    local vid="${2:?}";
    local failures=0

    run_chat_downloader "chat-logs/${outname?}" "${vid?}";

    until check_status "${outname}" "${vid}"; do
        # Likely broken by localization
        if grep -qF 'Private video' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Private video detected" >&2
            ((++failures))
            if ((failures > privatevideo_maxretries)); then
                echo "(downloader) giving up" >&2
                write_status "aborted" "${vid?}"
                exit $EXIT_FALSE
            fi
        elif grep -qF 'Members-only content' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Members only video detected, try again with cookies" >&2
            ((++failures))
            if ((failures > membersonlyvideo_maxretries)); then
                echo "(downloader) giving up" >&2
                write_status "aborted" "${vid?}"
                exit $EXIT_FALSE
            fi
        elif grep -qF 'video has been removed' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Removed video detected, giving up" >&2
            exit $EXIT_FALSE
        elif grep -qF 'Chat is disabled' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) Disabled chat detected" >&2
            ((++failures))
            if ((failures > chatlessvideo_maxretries)); then
                echo "(downloader) giving up" >&2
                write_status "aborted" "${vid?}"
                exit $EXIT_FALSE
            fi
        else
            ((++failures))
            if ((failures > other_maxretries)); then
                echo "(downloader) giving up" >&2
                write_status "aborted" "${vid?}"
                exit $EXIT_FALSE
            fi
        fi

        if grep -qF 'Finished retrieving chat replay' "chat-logs/${outname?}.stderr"; then
            echo "(downloader) warning: Chat replay detected, breaking out of here; fix this." >&2
            write_status "missed/bug" "${vid?}"
            break;
        fi

        echo '(downloader)' "Retrying chat downloader for video ${vid?}"
        run_chat_downloader "chat-logs/${outname?}" "${vid?}";
        sleep 60;
    done

    if grep -qF 'does not have a chat replay' "chat-logs/${outname?}.stderr"; then
        echo "(downloader) Disabled chat replay detected, giving up" >&2
        write_status "missed" "${vid?}"
        exit $EXIT_FALSE
    fi

    echo '(downloader)' "Finished downloading chat for video ${vid?}"

    cd chat-logs
    compress "${outname?}".json
    compress "${outname?}".stdout
    cd ..
    echo '(downloader)' "Finished compressing chat for video ${vid?}"
}

if (( $# == 2 )); then
    run_chat_downloader_waiting "${1:?}" "${2:?}"
else
    echo "usage: $0" '<outname>' '<video ID>'
    exit $EXIT_BADARG
fi
