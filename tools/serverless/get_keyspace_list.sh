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

#!/bin/bash

# PD get keyspace URL
PD_URL="https://serverless-cluster-pd.tidb-serverless.svc:2379/pd/api/v2/keyspaces?limit=100"

# init next page token
next_page_token=0

tmp_file="./tmp_ks_metas.txt"
res_file="./keyspace_meta_list.txt"

while true; do
    # Get keyspace meta list from PD http interface.
    URL="${PD_URL}&page_token=${next_page_token}"
    echo $URL
    curl --cacert /etc/secret-volume/ca.crt \
     --cert /etc/secret-volume/tls.crt \
     --key /etc/secret-volume/tls.key \
     "$URL" >$tmp_file

    # Append keyspace list to result file
    cat tmp_ks_metas.txt >> $res_file

    # Get new next page token
    next_page_token=`cat tmp_ks_metas.txt |jq -r '.next_page_token'`

    # Reached the last page, exiting the loop.
    if [[ "$next_page_token" == "null" || -z "$next_page_token" ]]; then
        echo "Received termination response. Exiting loop."
        break
    fi

    # Wait 1 sec.
    sleep 1
done

echo "result file:"$res_file

cat $res_file |grep -v "flag is from"| jq '.keyspaces[] | select(.state == "ENABLED" and (.config | has("serverless_cluster_id") | not)).name' >./non_cluster_ks.txt

cat $res_file |grep -v "flag is from" | jq -r '.keyspaces[] | select(.state=="ENABLED" and .config.serverless_cluster_id!="") | select(.config.serverless_cluster_id != null) | [.name, .config.serverless_cluster_id] | join(" ")' >./keyspace_name_cluster_list.txt
