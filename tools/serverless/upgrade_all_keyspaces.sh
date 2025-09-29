# Copyright 2024 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

op_num=$1

temp_dir_root_path="/tmp/upgrade_to_7.5/serverless_concurrent_upgrade/tmp_tidb_dir/$op_num"
echo "temp_dir_root_path:${temp_dir_root_path}"
mkdir -p $temp_dir_root_path

log_file_root_path="/tmp/upgrade_to_7.5/serverless_concurrent_upgrade/tmp_tidb_log/$op_num/"
echo "log_file_root_path:${log_file_root_path}"
mkdir -p $log_file_root_path

for line in `cat ks_list/keyspace_enable_name_list.non_vip.txt_$op_num |awk '{print $1}'`; do

ks_name=$(echo $line | awk '{print $1}')

echo "begin to upgrade keyspace name :${ks_name}"

dir="${temp_dir_root_path}/${ks_name}"
mkdir -p $dir

log_file="${log_file_root_path}/upgrade_${ks_name}.log"

### stop keyspace worker pod =================

curl -k "https://scaler-svc.tidb-worker.svc:9080/scaler/api/v1/admin/pause?keyspace_name=${ks_name}"

### start upgrade ==================================

/tidb-server -P 40$op_num \
--store=tikv \
--status=101${op_num} \
--path=serverless-cluster-pd.tidb-serverless.svc:2379 \
--log-file=${log_file} \
--store=tikv \
--enable-only-run-upgrade=true \
--temp-dir=${dir} \
--cluster-ca /etc/secret-volume/ca.crt \
--cluster-cert /etc/secret-volume/tls.crt \
--cluster-key /etc/secret-volume/tls.key \
--keyspace-name=${ks_name}
echo "keyspace_name:${ks_name} upgrade_res:$?"

### resume keyspace worker pod =================
curl -k "https://scaler-svc.tidb-worker.svc:9080/scaler/api/v1/admin/resume?keyspace_name=${ks_name}"

done
