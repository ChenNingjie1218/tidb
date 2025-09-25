// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package k8s

import (
	"fmt"
	"strings"
)

// PodDNSName returns the DNS name of a pod.
func PodDNSName(podIP string, namespace string) string {
	return fmt.Sprintf(
		"%s.%s.pod.cluster.local",
		strings.ReplaceAll(podIP, ".", "-"),
		namespace,
	)
}
