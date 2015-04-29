#!/bin/bash


DEST_EMAIL="infrastructure@adicu.com"

FROM_EMAIL="Wireless Processor <mailgun@sandbox8f6daca8215748e5b1a565fc428b7632.mailgun.org>"
DOMAIN="https://api.mailgun.net/v3/sandbox8f6daca8215748e5b1a565fc428b7632.mailgun.org"

SUBJECT="[logging] ERROR from wireless data processor"



if [ "$#" -ne 1 ]; then
    echo "Must pass name of a log file to monitor"
    exit 1
fi

LOG_FILE="$1"


send_email () {
    local LINE=$1
    echo "LINE: $LINE"

    curl -s --user "$MAILGUN_KEY" \
        "$DOMAIN/messages" \
        -F from="$FROM_EMAIL" \
        -F to="$DEST_EMAIL" \
        -F subject="$SUBJECT" \
        -F text="$(date)
ERROR:

$LINE"
}

echo "Watching $LOG_FILE for errors as 'ERROR'"
tail -f -n 0 "$LOG_FILE"  | while read LOGLINE
do
    if [[ "$LOGLINE" == *ERROR* ]];
    then
        send_email "$LOGLINE"
    fi
done

