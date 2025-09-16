#!/bin/bash
#
# Copyright 2022 PingCAP, Inc.
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

# Get package test list.
tasks=($(find . -iname "*_test.go" -exec dirname {} \; | sort -u))

res=()
pkg_index=1
bundled_parts=$1
target_part=$2

for t in ${tasks[@]}; do
  if [[ $t == "./cmd/ddltest"* ]]; then
    continue
  fi
  if [[ $t == "./br"* ]]; then
    continue
  fi
  if [[ $t == "./tests"* ]]; then
    continue
  fi
  pkg_index=$((pkg_index+1))
  [[ $((pkg_index % bundled_parts)) -eq $((target_part - 1)) ]] && res+=($t)
done

printf "%s " "${res[@]}"
