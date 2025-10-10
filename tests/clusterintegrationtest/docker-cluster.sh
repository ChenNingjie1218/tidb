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

# Start minio in background
/root/minio server /root/minio-data &

sleep 5

/root/.tiup/bin/tiup playground v7.3.0 --host 0.0.0.0 --mode=tidb-cse --tag serverless --without-monitor \
    --db.binpath /root/tidb-server \
    --kv.binpath /root/tikv-server \
    --tiflash.binpath /root/tiflash/tiflash \
    --tiflash.compute 1