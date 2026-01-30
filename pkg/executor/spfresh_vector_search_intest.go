//go:build intest

package executor

import "sync"

// ResetSPFreshVectorSearchForTest resets global caches used by SPFresh vector search.
//
// This is only available in intest builds to avoid exposing test-only APIs in production binaries.
func ResetSPFreshVectorSearchForTest() {
	spfreshClusterIDCache.Store(0)
	spfreshWorkerAddrByKeyspace = sync.Map{}
}
