#!/bin/bash
#
# Copyright 2024 PingCAP, Inc.
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

# Note: This file is supposed to run inside the docker (built by Dockerfile). Do not run it directly.

set -euo pipefail

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

# Start minio in background
/root/minio server /root/minio-data &

sleep 5

# Start tidb cluster in background
/root/.tiup/bin/tiup playground v7.3.0 --host 0.0.0.0 --mode=tidb-cse --tag serverless --without-monitor \
    --db.binpath /root/tidb-server \
    --kv.binpath /root/tikv-server \
    --tiflash.binpath /root/tiflash/tiflash \
    --tiflash.compute 1 &

function wait_for_tidb() {
  echo
  echo "+ Waiting TiDB start up"

  i=0
  while ! mysql -e 'show databases' -u root -h 127.0.0.1 --port 4000; do
    i=$((i + 1))
    if [[ "$i" -gt 30 ]]; then
      echo "* Fail to start TiDB"
      exit 1
    fi
    sleep 3
  done
  echo "  - TiDB startup successfully"
}

wait_for_tidb

echo "+ Wait 30s for TiFlash fully started"
sleep 30

echo "+ Running /root/mysql-tester"
/root/mysql-tester