#!/usr/bin/env bash
# Manage the local MySQL / MariaDB / Percona test servers, in one of two modes:
#
#   compose   the official images pinned in docker-compose.test.yml (the exact builds CI runs,
#             so local == CI); data in named docker volumes. On a developer host
#             docker-compose.local.yml joins it: no fsync per commit (docker/local.cnf), nothing
#             else differs. LV_COMPOSE_LOCAL=0 (or CI set, as on GitHub Actions) leaves it out.
#   tarball   official generic Linux x86_64 tarballs extracted under tmp/out/db/<flavor>/,
#             started directly (no Docker needed); data under tmp/out/servers/<flavor>/.
#
# Both modes use the same flavor names, ports, credentials and server settings, so the test
# harness does not care which one runs a flavor; `status` and `dsns` cover both.
#
# Usage: scripts/test-servers.sh <command> [flavor...]
#
#   compose up [--pre] [flavor...]   start containers and wait until healthy (default: the
#                                    blocking matrix; --pre adds the advisory pre-releases)
#   compose down [flavor...]         stop and remove containers (data volumes are kept)
#   compose clean [flavor...]        down, and drop the data volumes
#   compose logs <flavor>            server log
#   compose load-pre                 `docker load` the MySQL 26.10 EA image (no registry has it)
#   compose <docker compose args>    anything else goes to `docker compose -f docker-compose.test.yml`
#   fetch [flavor...]      tarball mode: download the pinned tarball (checksum verified) and
#                          install it as tmp/out/db/<flavor>. An older installed version is
#                          stopped and moved, with its instance directory, to
#                          tmp/out/db/<flavor>-<old version> and tmp/out/servers/<flavor>-<old version>;
#                          the new version starts with a fresh data directory (and is started if
#                          the old one was running).
#   start [flavor...]      tarball mode: start instances (default: the matrix flavors that are
#                          installed). Every instance takes about 1 GB of memory: start only what
#                          you test, e.g. `scripts/test-servers.sh start mysql267 mariadb130`.
#   stop [flavor...]       tarball mode: stop instances (default: every running one).
#   libs                   tarball mode: copy the shared libraries the MySQL tarballs need
#                          (libaio, libnuma, libncurses, libtinfo) out of the Percona image into
#                          tmp/out/lib, for hosts that lack them (they are then used automatically).
#   status [flavor...]     mode, state, port and version per flavor (default: matrix flavors plus
#                          installed/created others).
#   dsns [flavor...]       LAZYVIEW_TEST_DSNS value for the flavors that are up, in either mode
#                          (default: every flavor except the EOL ones).
#
# Test matrix (docs/server-setup.md "Supported servers"):
#   blocking, oldest supported LTS + latest GA:  mysql84 mysql267 percona84 percona97 mariadb1011 mariadb130
#   advisory, newest pre-release ("-pre"):       mariadb132-pre mysql2610-pre
# Outside the matrix, on request only (name them):
#   mysql97 mariadb114 mariadb118 mariadb123 (other vendor-supported lines; tarball mode only)
#   mysql80 percona80 (EOL, never in a default list; tarball mode only)
#
# Every server binds 127.0.0.1 on a fixed port (= its server_id), has ROW binlogging with FULL
# row images (plus GTIDs on MySQL/Percona), a 128M InnoDB buffer pool, a user lv/lv with ALL
# PRIVILEGES and a database `lvtest`.
#
# Env overrides: LV_DB_ROOT (tarball dir), LV_SERVERS_ROOT (instance dir), LV_LIB_DIR (extra
#                shared libraries for tarball binaries), LV_IMAGES_ROOT (downloaded image
#                tarballs), LV_START_TIMEOUT (seconds, default 180), LV_COMPOSE_LOCAL (0: compose
#                mode without docker-compose.local.yml, i.e. exactly the CI servers; default 1
#                unless CI is set).
set -euo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DB_ROOT=${LV_DB_ROOT:-$REPO/tmp/out/db}
SRV_ROOT=${LV_SERVERS_ROOT:-$REPO/tmp/out/servers}
LIB_DIR=${LV_LIB_DIR:-$REPO/tmp/out/lib}
IMAGES_ROOT=${LV_IMAGES_ROOT:-$REPO/tmp/out/images}
START_TIMEOUT=${LV_START_TIMEOUT:-180}
COMPOSE_FILE_PATH=$REPO/docker-compose.test.yml
COMPOSE_LOCAL_PATH=$REPO/docker-compose.local.yml

BLOCKING_FLAVORS=(mysql84 mysql267 percona84 percona97 mariadb1011 mariadb130)
PRE_FLAVORS=(mariadb132-pre mysql2610-pre)
MATRIX_FLAVORS=("${BLOCKING_FLAVORS[@]}" "${PRE_FLAVORS[@]}")
OTHER_FLAVORS=(mysql97 mariadb114 mariadb118 mariadb123)
EOL_FLAVORS=(mysql80 percona80)

# MySQL 26.10.0 Early Access (labs.mysql.com): the only container image is a `docker load`
# tarball; the name below is the one it loads as.
MYSQL2610_IMAGE=localhost/mysql/community-server:26.10.0-er1-amd64
MYSQL2610_DOCKER_TARBALL=mysql-community-server-26.10.0-labs-docker-el9-x86_64.tar.gz
MYSQL2610_DOCKER_SHA=f26559a00a022c3685089848a42958eb1a29f52fc2bb8b1b10594b77770d23d6
MYSQL2610_SNAPSHOTS=https://downloads.mysql.com/snapshots/pb/mysql-26.10.0-labs-release

port_of() {
  case $1 in
    mysql80) echo 33080 ;; mysql84) echo 33084 ;; mysql97) echo 33097 ;; mysql267) echo 33267 ;;
    mysql2610-pre) echo 33261 ;;
    mariadb1011) echo 33111 ;; mariadb114) echo 33114 ;; mariadb118) echo 33118 ;;
    mariadb123) echo 33123 ;; mariadb130) echo 33130 ;; mariadb132-pre) echo 33132 ;;
    percona80) echo 33180 ;; percona84) echo 33184 ;; percona97) echo 33197 ;;
    *) echo "unknown flavor: $1" >&2; return 1 ;;
  esac
}
server_id_of() { port_of "$1"; }   # ports are unique, reuse as server_id
is_mariadb() { [[ $1 == mariadb* ]]; }
in_list() { local x=$1; shift; local y; for y in "$@"; do [[ $x == "$y" ]] && return 0; done; return 1; }
has_compose_service() { in_list "$1" "${MATRIX_FLAVORS[@]}"; }

mysql_url() { echo "https://cdn.mysql.com/Downloads/MySQL-${1%.*}/mysql-$1-linux-$2-x86_64-minimal.tar.xz"; }
mysql_archive_url() { echo "https://cdn.mysql.com/archives/mysql-${1%.*}/mysql-$1-linux-$2-x86_64-minimal.tar.xz"; }
percona_url() {
  local line=${1%%-*}; line=${line%.*}
  echo "https://downloads.percona.com/downloads/Percona-Server-$line/Percona-Server-$1/binary/tarball/Percona-Server-$1-Linux.x86_64.glibc2.35-minimal.tar.gz"
}
mariadb_url() { echo "https://archive.mariadb.org/mariadb-$1/bintar-linux-systemd-x86_64/mariadb-$1-linux-systemd-x86_64.tar.gz"; }

# Pinned release per flavor: "<version> <sha256 of the tarball> <url> [fallback url]".
# MariaDB and Percona checksums are the vendors' published sha256 sums; MySQL publishes MD5
# only (the downloads matched it), the sha256 here is of that verified file.
# The image tags in docker-compose.test.yml pin the same builds.
pin_of() {
  local v
  case $1 in
    mysql80) v=8.0.46; echo "$v e2777d490fcaadc5a4be06db89b6935c78909bd9c3382af859ab284af0c950b0 $(mysql_url $v glibc2.17) $(mysql_archive_url $v glibc2.17)" ;;
    mysql84) v=8.4.11; echo "$v 383f54e124d5f325d67f0c6912a8f96814eedc761a17ea30112e52fa4cc6b143 $(mysql_url $v glibc2.28) $(mysql_archive_url $v glibc2.28)" ;;
    mysql97) v=9.7.2; echo "$v 6fe0baf13792e56afa3d93d6a174b4d5700901db1c8d09665225423dc094d0bb $(mysql_url $v glibc2.28) $(mysql_archive_url $v glibc2.28)" ;;
    mysql267) v=26.7.0; echo "$v 48a747f9e1591905a1e563ddd1c00818352b9201ca0911d66485c08614f4b3c2 $(mysql_url $v glibc2.28) $(mysql_archive_url $v glibc2.28)" ;;
    # Early Access build (not minimal: the labs snapshot has no minimal tarball).
    mysql2610-pre) v=26.10.0; echo "$v c5b0534c4d407455792bda7588b0d7ed97a82fae4f3d0490088c9220505143fa $MYSQL2610_SNAPSHOTS/mysql-26.10.0-er-linux-glibc2.28-x86_64.tar.gz" ;;
    percona80) v=8.0.46-37; echo "$v 9a76c3e9c71e3eb354bc066289086bd347cf24a3ac7c4fc2bc74788ec3ce0fa6 $(percona_url $v)" ;;
    percona84) v=8.4.11-11; echo "$v a8048701a124737ca816d1c098146703d0387109dbf519a333873ddf0a53dde3 $(percona_url $v)" ;;
    percona97) v=9.7.2-2; echo "$v 2e89167a066076efad6751add426826b6a157825eabef097fdd0e2acad6c59b9 $(percona_url $v)" ;;
    mariadb1011) v=10.11.19; echo "$v 7efb257927f31f4422f496246b32590d0b7617f3d9dd900e575de761b302d919 $(mariadb_url $v)" ;;
    mariadb114) v=11.4.13; echo "$v 3cebf63914df3154b7cb6d548337eb2be42c773aeb641110446b72e2746cfed7 $(mariadb_url $v)" ;;
    mariadb118) v=11.8.9; echo "$v 32f8871a2aa38b36aa418b7f4abc6fc0c6ef1ba5a45798cacb2afa52c8a6113b $(mariadb_url $v)" ;;
    mariadb123) v=12.3.3; echo "$v a12bf7bce08a896cdd01f174f04b4ce1d5da43ec250a5e7fceb9972119514329 $(mariadb_url $v)" ;;
    mariadb130) v=13.0.2; echo "$v e1b15581cfb2441fb7f3df2cc559998980406999e4c4a150ca303289f58b673c $(mariadb_url $v)" ;;
    mariadb132-pre) v=13.2.0; echo "$v 12879224974204f5218d8d272a141aea38517355c52777fd5e641971cd21bbeb $(mariadb_url $v)" ;;
    *) echo "unknown flavor: $1" >&2; return 1 ;;
  esac
}

# ---------------------------------------------------------------------------- tarball mode

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

# Tarball binaries find the libraries of `libs` (if any) through LD_LIBRARY_PATH.
if [[ -d $LIB_DIR ]]; then
  export LD_LIBRARY_PATH="$LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi

# Version of the installed server binary (`8.4.11`, `8.4.11-11`, `11.4.13`), empty if none.
installed_version() {
  available "$1" || return 0
  "$(server_bin "$1")" --no-defaults --version 2>/dev/null |
    sed -nE 's/.* Ver ([0-9][^ ]*).*/\1/p' | sed 's/-MariaDB.*//'
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
    # Many instances share the host with parallel test runs: keep memory small
    # (docker-compose.test.yml uses the same values).
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
      # On by default since 8.4.x / 9.x; mbind() is often not permitted in containers and
      # sandboxes, which floods the error log.
      echo "innodb_numa_interleave=OFF"
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

# Closes every inherited descriptor above 2 in the current (sub)shell, so that a daemon or a
# container started from here keeps nothing of the caller open: run under
# `flock <lock> scripts/test-servers.sh ...` it would otherwise hold the lock as long as it runs.
close_inherited_fds() {
  local fd n
  for fd in /proc/"$BASHPID"/fd/*; do
    n=${fd##*/}
    if [[ $n =~ ^[0-9]+$ ]] && (( n > 2 )); then eval "exec $n>&-"; fi
  done
}

start_one() {
  local f=$1 d; d=$(dir_of "$f")
  if ! available "$f"; then echo "[$f] not available (no binaries in $(base_of "$f"); run: $0 fetch $f)" >&2; return 1; fi
  if has_compose_service "$f" && compose_state "$f" | grep -q '^running'; then
    echo "[$f] already running as a container on port $(port_of "$f") (stop it first: $0 compose down $f)" >&2
    return 1
  fi
  if [[ ! $(installed_version "$f") ]]; then
    echo "[$f] $(server_bin "$f") does not run on this host:" >&2
    "$(server_bin "$f")" --no-defaults --version 2>&1 | head -3 >&2 || true
    echo "[$f] missing shared libraries can be copied out of the Percona image: $0 libs" >&2
    return 1
  fi
  init_instance "$f"
  if pid_alive "$f" && accepting "$f"; then
    setup_users "$f"
    echo "[$f] already running on port $(port_of "$f")"
    return 0
  fi
  if ! pid_alive "$f"; then
    rm -f "$(pidfile_of "$f")" "$(sock_of "$f")" "$(sock_of "$f").lock"
    echo "[$f] starting on 127.0.0.1:$(port_of "$f")"
    # The redirections cover the whole subshell (`scripts/test-servers.sh start | tail`
    # returns) and inherited descriptors are closed before the server replaces the subshell.
    (
      cd "$d" || exit 1
      close_inherited_fds
      exec setsid nohup "$(server_bin "$f")" --defaults-file="$d/my.cnf"
    ) >>"$d/stdout.log" 2>&1 </dev/null &
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

# Moves an old installation or instance directory aside as <dir>-<version>.
keep_aside() {
  local dir=$1 ver=$2
  [[ -e $dir ]] || return 0
  if [[ -e $dir-$ver ]]; then echo "cannot keep $dir: $dir-$ver already exists" >&2; return 1; fi
  mv "$dir" "$dir-$ver"
}

# Downloads $2 to $1 unless it is already there with sha256 $3 (several URLs may follow).
download_verified() {
  local file=$1 sha=$2; shift 2
  local u
  if [[ -f $file ]] && echo "$sha  $file" | sha256sum -c --status; then return 0; fi
  rm -f "$file"
  mkdir -p "$(dirname "$file")"
  for u in "$@"; do
    echo "downloading $u"
    if curl -fL --retry 3 -sS -o "$file.part" "$u"; then mv "$file.part" "$file"; break; fi
  done
  [[ -f $file ]] || { echo "download failed" >&2; return 1; }
  if ! echo "$sha  $file" | sha256sum -c --status; then
    echo "checksum mismatch for $file (want sha256 $sha)" >&2
    mv "$file" "$file.bad"
    return 1
  fi
}

fetch_one() {
  local f=$1 ver sha urls cur tarball stage base u was_running=0
  read -r ver sha urls <<<"$(pin_of "$f")"
  cur=$(installed_version "$f")
  if [[ $cur == "$ver" ]]; then echo "[$f] $ver installed"; return 0; fi
  base=$(base_of "$f")
  if [[ -z $cur && -e $base ]]; then echo "[$f] $base exists but holds no server binary (or it does not run here: $0 libs); remove it first" >&2; return 1; fi
  u=${urls%% *}
  tarball=$DB_ROOT/$f-$ver.tar.${u##*.}
  # shellcheck disable=SC2086 # the urls are space separated
  download_verified "$tarball" "$sha" $urls || { echo "[$f] download failed" >&2; return 1; }
  stage=$DB_ROOT/.stage/$f-$ver
  rm -rf "$stage" && mkdir -p "$stage"
  echo "[$f] extracting $tarball"
  tar -xf "$tarball" -C "$stage" --strip-components=1 --no-same-owner
  if [[ -n $cur ]]; then
    if pid_alive "$f"; then was_running=1; stop_one "$f"; fi
    keep_aside "$base" "$cur"
    keep_aside "$(dir_of "$f")" "$cur"
  fi
  mv "$stage" "$base"
  if [[ -n $cur ]]; then
    echo "[$f] installed $ver (was $cur, kept in $base-$cur and $(dir_of "$f")-$cur; delete them once $ver passes)"
  else
    echo "[$f] installed $ver"
  fi
  if (( was_running )); then start_one "$f"; fi
}

# The MySQL tarballs link libaio, libnuma and (the client) libncurses/libtinfo dynamically;
# hosts without them get the copies from the Percona image.
fetch_libs() {
  local image lib
  image=$(grep -oE 'percona/percona-server:[^ ]+' "$COMPOSE_FILE_PATH" | head -1)
  mkdir -p "$LIB_DIR"
  echo "copying libaio.so.1 libnuma.so.1 libncurses.so.6 libtinfo.so.6 from $image to $LIB_DIR"
  for lib in libaio.so.1 libnuma.so.1 libncurses.so.6 libtinfo.so.6; do
    docker run --rm --entrypoint cat "$image" "/usr/lib64/$lib" >"$LIB_DIR/$lib.part"
    mv "$LIB_DIR/$lib.part" "$LIB_DIR/$lib"
  done
  export LD_LIBRARY_PATH="$LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
}

tarball_running() { available "$1" && pid_alive "$1" && accepting "$1"; }

# ---------------------------------------------------------------------------- compose mode

# Whether docker-compose.local.yml (local speed settings) joins docker-compose.test.yml:
# LV_COMPOSE_LOCAL decides if set, otherwise yes unless CI is set (CI runs the durable defaults).
truthy() { [[ -n ${1:-} && $1 != 0 && $1 != false && $1 != no ]]; }
compose_local() {
  if [[ -n ${LV_COMPOSE_LOCAL:-} ]]; then truthy "$LV_COMPOSE_LOCAL"; return; fi
  if truthy "${CI:-}"; then return 1; fi
  return 0
}

# `docker compose` on docker-compose.test.yml (+ docker-compose.local.yml) with both profiles
# active (so that every verb sees the pre-release services; `compose_up` names the services it
# starts).
compose() {
  local files=(-f "$COMPOSE_FILE_PATH")
  if compose_local; then files+=(-f "$COMPOSE_LOCAL_PATH"); fi
  (
    close_inherited_fds
    exec docker compose "${files[@]}" --profile prerelease "$@"
  )
}

# "<state> <health>" of the flavor's container ("running healthy", "exited", ...), empty if none.
compose_state() {
  local cid
  cid=$(compose ps -aq "$1" 2>/dev/null | head -1)
  [[ -n $cid ]] || return 0
  docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' "$cid" 2>/dev/null || true
}
compose_healthy() { has_compose_service "$1" && [[ $(compose_state "$1") == "running healthy" ]]; }

# SELECT VERSION() through the container's own client over TCP as lv.
compose_version() {
  local client=mysql
  is_mariadb "$1" && client=mariadb
  compose exec -T "$1" "$client" --protocol=TCP -h127.0.0.1 -ulv -plv -NB -e 'SELECT VERSION()' 2>/dev/null
}

load_mysql2610_image() {
  if docker image inspect "$MYSQL2610_IMAGE" >/dev/null 2>&1; then
    echo "[mysql2610-pre] image $MYSQL2610_IMAGE is loaded"
    return 0
  fi
  local tarball=$IMAGES_ROOT/$MYSQL2610_DOCKER_TARBALL
  download_verified "$tarball" "$MYSQL2610_DOCKER_SHA" "$MYSQL2610_SNAPSHOTS/$MYSQL2610_DOCKER_TARBALL" || return 1
  echo "[mysql2610-pre] docker load $tarball"
  docker load -i "$tarball"
  docker image inspect "$MYSQL2610_IMAGE" >/dev/null 2>&1 ||
    { echo "[mysql2610-pre] the tarball did not load as $MYSQL2610_IMAGE; fix the name in $0 and docker-compose.test.yml" >&2; return 1; }
}

compose_up() {
  local pre=0 f
  if [[ ${1:-} == --pre ]]; then pre=1; shift; fi
  local flavors=("$@")
  if [[ ${#flavors[@]} -eq 0 ]]; then
    flavors=("${BLOCKING_FLAVORS[@]}")
    (( pre )) && flavors+=("${PRE_FLAVORS[@]}")
  fi
  for f in "${flavors[@]}"; do
    has_compose_service "$f" || { echo "[$f] has no service in docker-compose.test.yml (tarball mode only: $0 start $f)" >&2; return 1; }
    if pid_alive "$f"; then
      echo "[$f] a tarball instance (pid $(cat "$(pidfile_of "$f")")) holds port $(port_of "$f"); stop it first: $0 stop $f" >&2
      return 1
    fi
    if [[ $f == mysql2610-pre ]]; then load_mysql2610_image || return 1; fi
  done
  if compose_local; then
    echo "local speed settings on (docker-compose.local.yml: sync_binlog=0, innodb_flush_log_at_trx_commit=2; LV_COMPOSE_LOCAL=0 for the CI configuration)"
  else
    echo "CI configuration (durable defaults; docker-compose.local.yml left out)"
  fi
  compose up -d --wait --wait-timeout "$START_TIMEOUT" "${flavors[@]}"
  for f in "${flavors[@]}"; do echo "[$f] ready on 127.0.0.1:$(port_of "$f"), $(compose_version "$f")"; done
}

compose_down() {
  if [[ $# -eq 0 ]]; then compose down; else compose down "$@"; fi
}

compose_clean() {
  local f vol
  if [[ $# -eq 0 ]]; then compose down -v; return; fi
  compose down "$@"
  for f in "$@"; do
    vol=$(compose config --format json | docker_volume_name "$f")
    docker volume rm "$vol" >/dev/null 2>&1 && echo "[$f] removed volume $vol" || true
  done
}
# Resolves the named volume of a flavor from the compose config (lazyview_<flavor> by default).
docker_volume_name() {
  python3 -c 'import json,sys; print(json.load(sys.stdin)["volumes"][sys.argv[1]]["name"])' "$1" 2>/dev/null ||
    echo "lazyview_$1"
}

# ---------------------------------------------------------------------------- both modes

status_one() {
  local f=$1 port pinned note="" st; port=$(port_of "$f")
  pinned=$(pin_of "$f" | cut -d' ' -f1)
  if has_compose_service "$f"; then
    st=$(compose_state "$f")
    case $st in
      "running healthy")
        printf '%-15s %-6s compose running version=%s\n' "$f" "$port" "$(compose_version "$f")"; return 0 ;;
      running*)
        printf '%-15s %-6s compose starting/unhealthy (%s)\n' "$f" "$port" "$st"; return 0 ;;
      "") ;;
      *)
        # A stopped/failed container; a tarball instance may still own the port.
        if ! pid_alive "$f"; then printf '%-15s %-6s compose %s (%s)\n' "$f" "$port" "${st%% *}" "$0 compose logs $f"; return 0; fi ;;
    esac
  fi
  if ! available "$f"; then
    if has_compose_service "$f"; then printf '%-15s %-6s stopped (%s compose up %s)\n' "$f" "$port" "$0" "$f"
    else printf '%-15s %-6s not installed (pinned %s)\n' "$f" "$port" "$pinned"; fi
    return 0
  fi
  [[ $(installed_version "$f") == "$pinned" ]] || note=" (pinned $pinned: run $0 fetch $f)"
  if pid_alive "$f" && accepting "$f"; then
    printf '%-15s %-6s tarball running pid=%s version=%s%s\n' "$f" "$port" "$(cat "$(pidfile_of "$f")")" \
      "$(root_sql "$f" -e 'SELECT VERSION()' 2>/dev/null)" "$note"
  elif pid_alive "$f"; then
    printf '%-15s %-6s tarball starting/unresponsive pid=%s%s\n' "$f" "$port" "$(cat "$(pidfile_of "$f")")" "$note"
  else
    printf '%-15s %-6s stopped tarball version=%s%s\n' "$f" "$port" "$(installed_version "$f")" "$note"
  fi
}

dsns() {
  local out=() f
  for f in "$@"; do
    compose_healthy "$f" || tarball_running "$f" || continue
    out+=("$f=mysql://lv:lv@127.0.0.1:$(port_of "$f")/lvtest")
  done
  (IFS=,; echo "${out[*]}")
}

usage() {
  echo "usage: $0 compose up [--pre] [flavor...] | compose down|clean [flavor...] | compose logs <flavor> | compose load-pre" >&2
  echo "       $0 fetch|start|stop|status|dsns [flavor...] | libs" >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
cmd=$1; shift
if [[ $cmd == compose ]]; then
  [[ $# -ge 1 ]] || usage
  sub=$1; shift
  case $sub in
    up) compose_up "$@" ;;
    down) compose_down "$@" ;;
    clean) compose_clean "$@" ;;
    logs) compose logs --no-color "$@" ;;
    load-pre) load_mysql2610_image ;;
    *) compose "$sub" "$@" ;;
  esac
  exit $?
fi

explicit=0
if [[ $# -gt 0 ]]; then
  explicit=1
  flavors=("$@")
  for f in "${flavors[@]}"; do port_of "$f" >/dev/null || exit 2; done
fi

rc=0
case $cmd in
  fetch)
    (( explicit )) || flavors=("${MATRIX_FLAVORS[@]}")
    for f in "${flavors[@]}"; do fetch_one "$f" || rc=1; done ;;
  start)
    if (( ! explicit )); then
      flavors=()
      for f in "${MATRIX_FLAVORS[@]}"; do if available "$f"; then flavors+=("$f"); fi; done
    fi
    for f in "${flavors[@]}"; do start_one "$f" || rc=1; done ;;
  stop)
    (( explicit )) || flavors=("${MATRIX_FLAVORS[@]}" "${OTHER_FLAVORS[@]}" "${EOL_FLAVORS[@]}")
    for f in "${flavors[@]}"; do
      if (( explicit )) || pid_alive "$f"; then stop_one "$f" || rc=1; fi
    done ;;
  libs) fetch_libs ;;
  status)
    if (( ! explicit )); then
      flavors=("${MATRIX_FLAVORS[@]}")
      for f in "${OTHER_FLAVORS[@]}" "${EOL_FLAVORS[@]}"; do if available "$f"; then flavors+=("$f"); fi; done
    fi
    for f in "${flavors[@]}"; do status_one "$f"; done ;;
  dsns)
    (( explicit )) || flavors=("${MATRIX_FLAVORS[@]}" "${OTHER_FLAVORS[@]}")
    dsns "${flavors[@]}" ;;
  *) usage ;;
esac
exit $rc
