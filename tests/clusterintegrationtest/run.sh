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

set -euo pipefail

# Allow us to Ctrl+C outside CI
USE_TTY=""
test -t 1 && USE_TTY="-it"

# Reset WD
cd "$(dirname "$0")"

# WD=tidb, make a tidb server binary
echo "+ Building TiDB server with current source..."
pushd ../.. > /dev/null
make server
popd > /dev/null

# Don't use a new ID, to make sure previous build can be cleaned up.
IMAGE_ID=clusterintegrationtest:latest

# Make an image with TiDB+TiKV+TiFlash
cp ../../bin/tidb-server ./tidb-server
echo "+ Building container with other components..."
docker build --rm -t $IMAGE_ID .
echo
echo "+ Clean up previous builds..."
docker builder prune --force
echo "+ Run /root/docker-run.sh"
docker run -v clusterintegrationtest_tiup_cache:/root/.tiup/components --rm $USE_TTY $IMAGE_ID /bin/bash -c "/root/docker-run.sh"