#!/bin/bash

DEST_EMAIL="infrastructure@adicu.com"
FROM_EMAIL="Wireless Processor <mailgun@mg.adicu.com>"
DOMAIN="https://api.mailgun.net/v3/mg.adicu.com"
SUBJECT="[logging] ERROR from wireless data processor"

if [ "$MAILGUN_KEY" = "" ]
then
    echo "MAILGUN_KEY must be set in the environment"
    exit 1
elif [ "$#" -ne 1 ]; then
    echo "Must pass name of a log file to monitor"
    exit 1
fi

LOG_FILE="$1"


send_email () {
    local LINE=$1
    echo "LINE: $LINE"

    curl -s --user "api:$MAILGUN_KEY" \
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

