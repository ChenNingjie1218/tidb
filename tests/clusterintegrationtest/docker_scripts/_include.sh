#!/bin/bash
#
# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

function start_tidb_in_bg() {
  /root/.tiup/bin/tiup playground v7.3.0 --host 0.0.0.0 --mode=tidb-cse --tag serverless --without-monitor \
    --db.binpath /root/tidb-server \
    --kv.binpath /root/tikv-server \
    --tiflash.binpath /root/tiflash/tiflash \
    --tiflash.compute 1 &
}

function start_tidb_in_fg() {
  /root/.tiup/bin/tiup playground v7.3.0 --host 0.0.0.0 --mode=tidb-cse --tag serverless --without-monitor \
    --db.binpath /root/tidb-server \
    --kv.binpath /root/tikv-server \
    --tiflash.binpath /root/tiflash/tiflash \
    --tiflash.compute 1
}

function wait_for_tidb() {
  echo
  echo "+ Waiting TiDB start up"

  for i in {1..30}; do
    if mysql -e 'show databases' -u root -h 127.0.0.1 --port 4000; then
      echo "  - TiDB startup successfully"
      return
    fi
    sleep 3
  done
  echo "* Fail to start TiDB cluster in 900s"
  exit 1
}

function stop_tiup() {
  echo "+ Stopping TiUP"
  TIUP_PID=$(pgrep -f "tiup-playground")
  if [ -n "$TIUP_PID" ]; then
    echo "  - Sending SIGTERM to PID=$TIUP_PID"
    kill $TIUP_PID
  fi

  for i in {1..30}; do
    if ! pgrep -f "tiup-playground" > /dev/null; then
      echo "  - TiUP stopped successfully"
      return
    fi
    sleep 1
  done

  echo "* Fail to stop TiUP in 30s"
  exit 1
}

function wait_for_tiflash() {
  echo
  echo "+ Waiting TiFlash start up (30s)"
  sleep 30
}

function print_versions() {
  # Print versions
  echo "+ TiDB Version"
  /root/tidb-server -V
  echo
  echo "+ TiKV Version"
  /root/tikv-server --version
  echo
  echo "+ TiFlash Version"
  /root/tiflash/tiflash version
  echo
  echo "+ minio Version"
  /root/minio -v
  echo
  echo "+ TiUP Version"
  /root/.tiup/bin/tiup playground -v
}