#!/bin/sh
# Starts a MySQL 8.4 Group Replication member of the `gr84` family with its address registered as an
# IP, so the other members can reach it. Mounted read-only by the gr84 services in
# docker-compose.test.yml and invoked as the service's entrypoint:
#     entrypoint: ["/bin/sh", "/lv-report-host.sh"]
# Compose then appends the service's `command` list, which is where the mysqld flags live, and this
# script hands them on to the image's own entrypoint unchanged.
#
# Why the address has to be given at all (measured on this host 2026-11-27T20:52Z with the family up on
# Docker's default bridge, tmp/out/lv-worker-gtid-e/gr84-probe/probe.log; lv-super's ruling of
# 2026-11-27, msg_7fdf803f9a34: keep `network_mode: bridge`, register an IP):
#
#   [ERROR] [MY-010584] [Repl] Replica I/O for channel 'group_replication_recovery': Error connecting to
#     source 'lvdoc@c4e2fb1bac3f:3306'. ... Message: Unknown MySQL server host 'c4e2fb1bac3f' (-2),
#     Error_code: MY-002005
#   [ERROR] [MY-011582] [Repl] Plugin group_replication reported: 'There was an error when connecting to
#     the donor server. Please check that group_replication_recovery channel credentials and all
#     MEMBER_HOST column values of performance_schema.replication_group_members table are correct and DNS
#     resolvable.'
#
# Group Replication registers the member in performance_schema.replication_group_members under its
# report_host, which defaults to the container's hostname (its container id here), and distributed
# recovery connects to MEMBER_HOST:3306. The default bridge has no embedded DNS, so that name resolves
# nowhere and every joiner sits in RECOVERING for ever. On a compose project network Docker's own DNS
# answers it, which is why the same family was green before it moved to the bridge.
#
# report_host is startup-only - it is not in mysqld's dynamic set, so it cannot be fixed with SET GLOBAL
# after the member is up - and the container's docker0 address is only known once it is running, so the
# address has to be put on the start command, which is what this script does.
#
# The address is read from the /etc/hosts line Docker writes for the container's own hostname
# (`172.17.0.5 c7e878080d54`, measured in the pinned image) rather than from `hostname -I`, both because
# this image ships no `hostname` binary at all (measured: `hostname: command not found`) and because the
# hosts line is what Docker itself uses. Failing loudly here is deliberate: a member that starts without
# a usable report_host registers a hostname, and the failure then surfaces 5 minutes later as joiners
# stuck in RECOVERING, which is a much worse thing to debug. The runner (tmp/lvs-cluster.sh) and the CI
# lane both assert that every MEMBER_HOST is an IPv4 address, so this cannot rot back either.
set -eu

name=$(cat /proc/sys/kernel/hostname)
ip=$(grep -w -- "$name" /etc/hosts | awk '{print $1; exit}')
if [ -z "$ip" ]; then
    echo "lv-report-host: /etc/hosts has no address for hostname [$name]" >&2
    cat /etc/hosts >&2
    exit 1
fi
# 127.0.0.1 would register a member nobody else can reach either; refuse it rather than reproduce the
# same unreachable-member failure through a different door.
case $ip in
    127.*)
        echo "lv-report-host: [$name] resolves to the loopback address [$ip], which no other member can reach" >&2
        exit 1
        ;;
esac

# /usr/local/bin/docker-entrypoint.sh is the image's own entrypoint (verified against the pinned image's
# Config.Entrypoint), so /docker-entrypoint-initdb.d still runs. "$@" is the service's `command` list:
# the mysqld flags from the compose file, which the entrypoint appends to its own argument list.
exec /usr/local/bin/docker-entrypoint.sh mysqld --report-host="$ip" "$@"
