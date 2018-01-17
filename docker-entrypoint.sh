#!/bin/sh

if [ -z "$CKAN_CLIENT_INTERVAL" ]; then
  CKAN_CLIENT_INTERVAL="0 */3 * * *"
fi

if [ -z "$CKAN_CLIENT_INTERVAL_BULK" ]; then
  CKAN_CLIENT_INTERVAL_BULK="0 0 * * 0"
fi

> /etc/crontabs/root
echo "$CKAN_CLIENT_INTERVAL python2 /app/ckanclient.py" >> /etc/crontabs/root
echo "$CKAN_CLIENT_INTERVAL_BULK python2 /app/sdsclient.py" >> /etc/crontabs/root
echo "root" > /etc/crontabs/cron.update

exec "$@"
