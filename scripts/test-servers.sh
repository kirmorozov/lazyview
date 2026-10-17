#!/usr/bin/env bash
# Manage local MySQL / MariaDB / Percona test server instances started directly
# from official tarballs (extracted under tmp/out/db/<flavor>/).
#
# Usage: scripts/test-servers.sh start|stop|status|dsns [flavor...]
#   flavors: mysql80 mysql84 mariadb1011 mariadb114 percona80 percona84
#   default: all flavors whose tarball is extracted.
#
# Each instance lives in tmp/out/servers/<flavor>/ (my.cnf, data/, binlogs,
# error log, socket, pid file), binds 127.0.0.1 on a fixed port, has ROW
# binlogging enabled (plus GTIDs on MySQL/Percona) and a user lv/lv with ALL
# PRIVILEGES and a database `lvtest`.
#
# Env overrides: LV_DB_ROOT (tarball dir), LV_SERVERS_ROOT (instance dir),
#                LV_START_TIMEOUT (seconds, default 180).
set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DB_ROOT=${LV_DB_ROOT:-$REPO/tmp/out/db}
SRV_ROOT=${LV_SERVERS_ROOT:-$REPO/tmp/out/servers}
START_TIMEOUT=${LV_START_TIMEOUT:-180}

ALL_FLAVORS=(mysql80 mysql84 mariadb1011 mariadb114 percona80 percona84)

port_of() {
  case $1 in
    mysql80) echo 33080 ;; mysql84) echo 33084 ;;
    mariadb1011) echo 33111 ;; mariadb114) echo 33114 ;;
    percona80) echo 33180 ;; percona84) echo 33184 ;;
    *) echo "unknown flavor: $1" >&2; return 1 ;;
  esac
}
server_id_of() { port_of "$1"; }   # ports are unique, reuse as server_id
is_mariadb() { [[ $1 == mariadb* ]]; }

base_of() { echo "$DB_ROOT/$1"; }
dir_of() { echo "$SRV_ROOT/$1"; }
sock_of() { echo "$(dir_of "$1")/mysqld.sock"; }
pidfile_of() { echo "$(dir_of "$1")/mysqld.pid"; }

available() { [[ -x "$(base_of "$1")/bin/mysqld" || -x "$(base_of "$1")/bin/mariadbd" ]]; }

server_bin() {
  local b; b=$(base_of "$1")
  if [[ -x $b/bin/mariadbd ]]; then echo "$b/bin/mariadbd"; else echo "$b/bin/mysqld"; fi
}
client_bin() {
  local b; b=$(base_of "$1")
  if [[ -x $b/bin/mariadb ]]; then echo "$b/bin/mariadb"; else echo "$b/bin/mysql"; fi
}

# Run SQL as root over the instance's unix socket.
root_sql() {
  local f=$1; shift
  "$(client_bin "$f")" --no-defaults -uroot --protocol=SOCKET -S "$(sock_of "$f")" -N -B "$@"
}

pid_alive() {
  local pf; pf=$(pidfile_of "$1")
  [[ -s $pf ]] && kill -0 "$(cat "$pf")" 2>/dev/null
}

accepting() { root_sql "$1" -e 'SELECT 1' >/dev/null 2>&1; }

write_cnf() {
  local f=$1 d b port
  d=$(dir_of "$f"); b=$(base_of "$f"); port=$(port_of "$f")
  {
    echo "[mysqld]"
    echo "basedir=$b"
    echo "datadir=$d/data"
    echo "socket=$d/mysqld.sock"
    echo "pid-file=$d/mysqld.pid"
    echo "log-error=$d/error.log"
    echo "tmpdir=$d/tmp"
    echo "port=$port"
    echo "bind-address=127.0.0.1"
    echo "skip-name-resolve"
    echo "server_id=$(server_id_of "$f")"
    echo "log_bin=$d/data/binlog"
    echo "binlog_format=ROW"
    echo "binlog_row_image=FULL"
    echo "innodb_buffer_pool_size=128M"
    echo "max_connections=200"
    if [[ $(id -u) -eq 0 ]]; then echo "user=root"; fi
    if is_mariadb "$f"; then
      echo "log_basename=mariadb"
    else
      echo "gtid_mode=ON"
      echo "enforce_gtid_consistency=ON"
      echo "mysqlx=OFF"
      echo "secure_file_priv=$d/tmp"
    fi
  } >"$d/my.cnf"
}

init_instance() {
  local f=$1 d b
  d=$(dir_of "$f"); b=$(base_of "$f")
  mkdir -p "$d/tmp"
  write_cnf "$f"
  [[ -d $d/data/mysql ]] && return 0
  rm -rf "$d/data"
  echo "[$f] initializing data directory"
  if is_mariadb "$f"; then
    local inst=$b/scripts/mariadb-install-db
    [[ -x $inst ]] || inst=$b/scripts/mysql_install_db
    "$inst" --defaults-file="$d/my.cnf" --basedir="$b" --datadir="$d/data" \
      --auth-root-authentication-method=normal --skip-test-db \
      >"$d/init.log" 2>&1 || { echo "[$f] init failed, see $d/init.log" >&2; tail -20 "$d/init.log" >&2; return 1; }
  else
    mkdir -p "$d/data" && rmdir "$d/data"
    "$(server_bin "$f")" --defaults-file="$d/my.cnf" --initialize-insecure \
      >"$d/init.log" 2>&1 || { echo "[$f] init failed, see $d/init.log / $d/error.log" >&2; tail -20 "$d/error.log" >&2; return 1; }
  fi
  rm -f "$d/.setup-done"
}

setup_users() {
  local f=$1 d; d=$(dir_of "$f")
  [[ -f $d/.setup-done ]] && return 0
  echo "[$f] creating user lv and database lvtest"
  if is_mariadb "$f"; then
    # Drop anonymous accounts which could shadow lv.
    local h
    while IFS= read -r h; do
      root_sql "$f" -e "DROP USER IF EXISTS ''@'$h'"
    done < <(root_sql "$f" -e "SELECT Host FROM mysql.user WHERE User=''")
  fi
  root_sql "$f" -e "
    CREATE USER IF NOT EXISTS 'lv'@'%' IDENTIFIED BY 'lv';
    GRANT ALL PRIVILEGES ON *.* TO 'lv'@'%' WITH GRANT OPTION;
    CREATE DATABASE IF NOT EXISTS lvtest;
    FLUSH PRIVILEGES;"
  touch "$d/.setup-done"
}

start_one() {
  local f=$1 d; d=$(dir_of "$f")
  if ! available "$f"; then echo "[$f] not available (no binaries in $(base_of "$f"))" >&2; return 1; fi
  init_instance "$f"
  if pid_alive "$f" && accepting "$f"; then
    setup_users "$f"
    echo "[$f] already running on port $(port_of "$f")"
    return 0
  fi
  if ! pid_alive "$f"; then
    rm -f "$(pidfile_of "$f")" "$(sock_of "$f")" "$(sock_of "$f").lock"
    echo "[$f] starting on 127.0.0.1:$(port_of "$f")"
    (cd "$d" && setsid nohup "$(server_bin "$f")" --defaults-file="$d/my.cnf" \
      >>"$d/stdout.log" 2>&1 </dev/null &)
  fi
  local i=0
  until accepting "$f"; do
    i=$((i + 1))
    if (( i > START_TIMEOUT * 2 )); then
      echo "[$f] did not accept connections within ${START_TIMEOUT}s; tail of $d/error.log:" >&2
      tail -30 "$d/error.log" >&2 || true
      return 1
    fi
    if (( i > 10 )) && ! pid_alive "$f" && ! pgrep -f -- "--defaults-file=$d/my.cnf" >/dev/null; then
      echo "[$f] server exited during startup; tail of $d/error.log:" >&2
      tail -30 "$d/error.log" >&2 || true
      return 1
    fi
    sleep 0.5
  done
  setup_users "$f"
  echo "[$f] ready"
}

stop_one() {
  local f=$1 pf pid i=0
  pf=$(pidfile_of "$f")
  if ! pid_alive "$f"; then echo "[$f] not running"; return 0; fi
  pid=$(cat "$pf")
  echo "[$f] stopping (pid $pid)"
  kill -TERM "$pid" 2>/dev/null || true
  while kill -0 "$pid" 2>/dev/null; do
    i=$((i + 1))
    if (( i > 240 )); then echo "[$f] still alive after 120s, sending SIGKILL" >&2; kill -KILL "$pid" 2>/dev/null || true; break; fi
    sleep 0.5
  done
  echo "[$f] stopped"
}

status_one() {
  local f=$1 port; port=$(port_of "$f")
  if ! available "$f"; then printf '%-12s %-6s %s\n' "$f" "$port" "unavailable"; return 0; fi
  if pid_alive "$f" && accepting "$f"; then
    printf '%-12s %-6s running pid=%s version=%s\n' "$f" "$port" "$(cat "$(pidfile_of "$f")")" \
      "$(root_sql "$f" -e 'SELECT VERSION()' 2>/dev/null)"
  elif pid_alive "$f"; then
    printf '%-12s %-6s starting/unresponsive pid=%s\n' "$f" "$port" "$(cat "$(pidfile_of "$f")")"
  else
    printf '%-12s %-6s stopped\n' "$f" "$port"
  fi
}

dsns() {
  local out=() f
  for f in "$@"; do
    available "$f" && pid_alive "$f" && accepting "$f" || continue
    out+=("$f=mysql://lv:lv@127.0.0.1:$(port_of "$f")/lvtest")
  done
  (IFS=,; echo "${out[*]}")
}

usage() { echo "usage: $0 start|stop|status|dsns [flavor...]" >&2; exit 2; }

[[ $# -ge 1 ]] || usage
cmd=$1; shift
if [[ $# -gt 0 ]]; then
  flavors=("$@")
  for f in "${flavors[@]}"; do port_of "$f" >/dev/null || exit 2; done
else
  flavors=()
  for f in "${ALL_FLAVORS[@]}"; do
    if available "$f"; then flavors+=("$f"); fi
  done
fi

rc=0
case $cmd in
  start)  for f in "${flavors[@]}"; do start_one "$f" || rc=1; done ;;
  stop)   for f in "${flavors[@]}"; do stop_one "$f" || rc=1; done ;;
  status) for f in "${flavors[@]}"; do status_one "$f"; done ;;
  dsns)   dsns "${flavors[@]}" ;;
  *) usage ;;
esac
exit $rc
