// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gctuner

import (
	"math"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/memory"
)

var (
	maxGCPercent atomic.Uint32
	minGCPercent atomic.Uint32

	// EnableGOGCTuner is to control whether enable the GOGC tuner.
	EnableGOGCTuner  atomic.Bool
	defaultGCPercent uint32 = 100
)

const (
	defaultMaxGCPercent uint32 = 500
	defaultMinGCPercent uint32 = 100
)

// SetMaxGCPercent sets the max cost of memory.
func SetMaxGCPercent(percent uint32) {
	maxGCPercent.Store(percent)
}

// MaxGCPercent get the max cost of memory.
func MaxGCPercent() uint32 {
	return maxGCPercent.Load()
}

// SetMinGCPercent sets the max cost of memory.
func SetMinGCPercent(percent uint32) {
	minGCPercent.Store(percent)
}

// MinGCPercent get the min cost of memory.
func MinGCPercent() uint32 {
	return minGCPercent.Load()
}

func init() {
	if val, err := strconv.Atoi(os.Getenv("GOGC")); err == nil {
		defaultGCPercent = uint32(val)
	}
	SetMinGCPercent(defaultMinGCPercent)
	SetMaxGCPercent(defaultMaxGCPercent)
}

// SetDefaultGOGC is to set the default GOGC value.
func SetDefaultGOGC() {
	util.SetGOGC(int(defaultGCPercent))
}

// Tuning sets the threshold of heap which will be respect by gogc tuner.
// When Tuning, the env GOGC will not be take effect.
// threshold: disable tuning if threshold == 0
func Tuning(threshold uint32) {
	// disable gc tuner if percent is zero
	if threshold <= 0 && globalTuner != nil {
		globalTuner.stop()
		globalTuner = nil
		return
	}

	if globalTuner == nil {
		globalTuner = newTuner(threshold)
		return
	}
	globalTuner.setThreshold(threshold)
}

// GetGOGC returns the current GCPercent.
func GetGOGC() uint32 {
	if globalTuner == nil {
		return defaultGCPercent
	}
	return globalTuner.getGCPercent()
}

// only allow one gc tuner in one process
// It is not thread-safe. so it is a private, singleton pattern to avoid misuse.
var globalTuner *tuner

/*
// 			Heap
//  _______________  => limit: host/cgroup memory hard limit
// |               |
// |---------------| => threshold: increase GCPercent when gc_trigger < threshold
// |               |
// |---------------| => gc_trigger: heap_live + heap_live * GCPercent / 100
// |               |
// |---------------|
// |   heap_live   |
// |_______________|
*/
// Go runtime only trigger GC when hit gc_trigger which affected by GCPercent and heap_live.
// So we can change GCPercent dynamically to tuning GC performance.
type tuner struct {
	finalizer *finalizer
	gcPercent atomic.Uint32
	threshold atomic.Uint32 // high water level, percent of total memory.
}

func newTuner(threshold uint32) *tuner {
	t := &tuner{}
	t.gcPercent.Store(defaultGCPercent)
	t.threshold.Store(threshold)
	t.finalizer = newFinalizer(t.tuning) // start tuning
	return t
}

func (t *tuner) stop() {
	t.finalizer.stop()
}

func (t *tuner) setThreshold(threshold uint32) {
	t.threshold.Store(threshold)
}

func (t *tuner) getThresholdBytes() uint64 {
	memTotal := memory.ServerMemoryLimit.Load()
	if memTotal == 0 {
		var err error
		memTotal, err = memory.MemTotal()
		if err != nil {
			// unlikely to happen, return 0 to stop tuning
			return 0
		}
	}
	return uint64(t.threshold.Load()) * memTotal / 100
}

func (t *tuner) setGCPercent(percent uint32) uint32 {
	result := uint32(util.SetGOGC(int(percent)))
	t.gcPercent.Store(result)
	return result
}

func (t *tuner) getGCPercent() uint32 {
	return t.gcPercent.Load()
}

// tuning check the memory inuse and tune GC percent dynamically.
// Go runtime ensure that it will be called serially.
func (t *tuner) tuning() {
	if !EnableGOGCTuner.Load() {
		return
	}

	inuse := readMemoryInuse()
	threshold := t.getThresholdBytes()
	// stop gc tuning
	if threshold <= 0 {
		return
	}

	t.setGCPercent(calcGCPercent(inuse, threshold))
}

// threshold = inuse + inuse * (gcPercent / 100)
// => gcPercent = (threshold - inuse) / inuse * 100
// if threshold < inuse*2, so gcPercent < 100, and GC positively to avoid OOM
// if threshold > inuse*2, so gcPercent > 100, and GC negatively to reduce GC times
func calcGCPercent(inuse, threshold uint64) uint32 {
	// invalid params
	if inuse == 0 || threshold == 0 {
		return defaultGCPercent
	}
	// inuse heap larger than threshold, use min percent
	if threshold <= inuse {
		return minGCPercent.Load()
	}
	gcPercent := uint32(math.Floor(float64(threshold-inuse) / float64(inuse) * 100))
	if gcPercent < minGCPercent.Load() {
		return minGCPercent.Load()
	} else if gcPercent > maxGCPercent.Load() {
		return maxGCPercent.Load()
	}
	return gcPercent
}
