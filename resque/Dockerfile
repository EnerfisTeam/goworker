FROM docker.io/redis:3.2.5

COPY redis.conf /usr/local/etc/redis/redis.conf
COPY redis-entrypoint.sh /usr/local/bin/
ENTRYPOINT [ "redis-entrypoint.sh" ]
