#!/bin/sh

sed -i "s/\$REDIS_PASS/$REDIS_PASS/g" /usr/local/etc/redis/redis.conf

redis-server /usr/local/etc/redis/redis.conf $REDIS_SLAVEOF

