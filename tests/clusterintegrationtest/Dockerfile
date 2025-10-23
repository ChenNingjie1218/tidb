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

FROM hub.pingcap.net/keyspace/tiflash:v8.5.0 AS image-tiflash

FROM hub.pingcap.net/keyspace/tikv-cse:v7.5.0 AS image-tikv

FROM hub.pingcap.net/keyspace/tidb-cse-cluster-integration-test-base:latest
COPY --from=image-tiflash /tiflash /root/tiflash
COPY --from=image-tikv /tikv-server /root/tikv-server
COPY --from=image-tikv /tikv-worker /root/tikv-worker
COPY ./tidb-server /root/tidb-server
COPY ./r /root/r
COPY ./t /root/t
COPY ./python_testers /root/python_testers
COPY ./config /root/config
COPY --chmod=755 ./docker_scripts /root/docker_scripts