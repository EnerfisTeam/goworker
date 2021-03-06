#!/bin/sh

sed -i "s/\$SENTINEL_QUORUM/$SENTINEL_QUORUM/g" /usr/local/etc/redis/sentinel.conf
sed -i "s/\$SENTINEL_DOWN_AFTER/$SENTINEL_DOWN_AFTER/g" /usr/local/etc/redis/sentinel.conf
sed -i "s/\$SENTINEL_FAILOVER/$SENTINEL_FAILOVER/g" /usr/local/etc/redis/sentinel.conf
sed -i "s/\$SENTINEL_MASTER_NAME/$SENTINEL_MASTER_NAME/g" /usr/local/etc/redis/sentinel.conf
sed -i "s/\$SENTINEL_MASTER_PASS/$SENTINEL_MASTER_PASS/g" /usr/local/etc/redis/sentinel.conf

redis-server /usr/local/etc/redis/sentinel.conf --sentinel
