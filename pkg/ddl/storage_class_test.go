// Copyright 2025 PingCAP, Inc.
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

package ddl_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestBuildStorageClassSettingsFromJSON(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name   string
		input  string
		expect *model.StorageClassSettings
	}{
		{
			name:  "valid string tier",
			input: `"STANDARD"`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD"},
				},
			},
		},
		{
			name:   "invalid string tier",
			input:  `"INVALID"`,
			expect: nil,
		},
		{
			name: "valid no scope",
			input: `{
				"tier": "STANDARD"
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD"},
				},
			},
		},
		{
			name: "valid names in",
			input: `{
				"tier": "STANDARD",
				"names_in": ["part1", "part2"]
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{
						Tier:    "STANDARD",
						NamesIn: []string{"part1", "part2"},
					},
				},
			},
		},
		{
			name: "valid less than",
			input: `{
				"tier": "STANDARD",
				"less_than": "100"
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{
						Tier:     "STANDARD",
						LessThan: stringPtr("100"),
					},
				},
			},
		},
		{
			name: "valid values in",
			input: `{
				"tier": "STANDARD",
				"values_in": ["100", "200"]
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{
						Tier:     "STANDARD",
						ValuesIn: []string{"100", "200"},
					},
				},
			},
		},
		{
			name: "invalid multiple scopes",
			input: `{
				"tier": "STANDARD",
				"names_in": ["part1", "part2"],
				"values_in": ["100", "200"]
			}`,
			expect: nil,
		},
		{
			name: "invalid unknown field",
			input: `{
				"tier": "STANDARD",
				"unknown": "100"
			}`,
			expect: nil,
		},
		{
			name: "invalid JSON",
			input: `{
				"tier": "STANDARD",
				"names_in": ["part1", "part2"
			}`,
			expect: nil,
		},
		{
			name: "multiple tiers",
			input: `[
				{"tier": "IA", "names_in": ["part1", "part2"]},
				{"tier": "STANDARD"}
			]`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1", "part2"}},
					{Tier: "STANDARD"},
				},
			},
		},
		{
			name: "valid transitions",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 30}]
			}`,
			expect: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD", Transitions: []model.StorageClassTransitRule{
						{Tier: "IA", AfterDays: 30},
					}},
				},
			},
		},
		{
			name: "redundant transitions",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 30}, {"tier": "IA", "after_days": 60}]
			}`,
			expect: nil,
		},
		{
			name: "transitions from cold to hot",
			input: `{
				"tier": "IA",
				"transitions": [{"tier": "STANDARD", "after_days": 30}]
			}`,
			expect: nil,
		},
		{
			name: "transitions from cold to hot 2",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 15}, {"tier": "STANDARD", "after_days": 30}]
			}`,
			expect: nil,
		},
		{
			name: "transitions with transit time of 0",
			input: `{
				"tier": "STANDARD",
				"transitions": [{"tier": "IA", "after_days": 0}]
			}`,
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ddl.BuildStorageClassSettingsFromJSON(json.RawMessage([]byte(tt.input)))
			if tt.expect != nil {
				require.NoError(err)
				require.Equal(tt.expect, got)
			} else {
				require.Error(err)
			}
		})
	}

	got, err := ddl.BuildStorageClassSettingsFromJSON(nil)
	require.NoError(err)
	expect := &model.StorageClassSettings{
		Defs: []*model.StorageClassDef{
			{Tier: "STANDARD"},
		},
	}
	require.Equal(expect, got)
}

func TestBuildStorageClassForTable(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name     string
		settings *model.StorageClassSettings
		expected string
	}{
		{
			name:     "no storage class settings",
			settings: nil,
			expected: "",
		},
		{
			name: "no scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA"},
				},
			},
			expected: "IA",
		},
		{
			name: "no matching scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1"}},
				},
			},
			expected: "STANDARD",
		},
		{
			name: "multiply tiers",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD", NamesIn: []string{"part1"}},
					{Tier: "STANDARD", NamesIn: []string{"part2"}},
					{Tier: "IA"},
				},
			},
			expected: "IA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbInfo := &model.TableInfo{}
			err := ddl.BuildStorageClassForTable(tbInfo, tt.settings)
			require.NoError(err)
			require.Equal(tt.expected, tbInfo.StorageClassTier)
		})
	}
}

func TestBuildStorageClassForPartitions(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name       string
		settings   *model.StorageClassSettings
		partitions []model.PartitionDefinition
		expected   []string
	}{
		{
			name:     "no storage class settings",
			settings: nil,
			partitions: []model.PartitionDefinition{
				{Name: pmodel.CIStr{L: "part1"}},
				{Name: pmodel.CIStr{L: "part2"}},
			},
			expected: []string{"", ""},
		},
		{
			name: "no scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA"},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: pmodel.CIStr{L: "part1"}},
				{Name: pmodel.CIStr{L: "part2"}},
			},
			expected: []string{"IA", "IA"},
		},
		{
			name: "names_in scope definition",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "IA", NamesIn: []string{"part1"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: pmodel.CIStr{L: "part1"}},
				{Name: pmodel.CIStr{L: "part2"}},
			},
			expected: []string{"IA", "STANDARD"},
		},
		{
			name: "multiple tiers",
			settings: &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{
					{Tier: "STANDARD", NamesIn: []string{"part1"}},
					{Tier: "IA"},
					{Tier: "STANDARD", NamesIn: []string{"part2"}},
				},
			},
			partitions: []model.PartitionDefinition{
				{Name: pmodel.CIStr{L: "part1"}},
				{Name: pmodel.CIStr{L: "part2"}},
			},
			expected: []string{"STANDARD", "IA"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbInfo := &model.TableInfo{
				Partition: &model.PartitionInfo{
					Definitions: tt.partitions,
				},
			}
			err := ddl.BuildStorageClassForPartitions(tt.partitions, tbInfo, tt.settings)
			require.NoError(err)
			for i, part := range tbInfo.Partition.Definitions {
				require.Equal(tt.expected[i], part.StorageClassTier)
			}
		})
	}
}

func TestStorageClassString(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		name        string
		tier        string
		transitions []model.StorageClassTransitRule
		expected    string
	}{
		{
			name:     "no transitions",
			tier:     "STANDARD",
			expected: "STANDARD",
		},
		{
			name:     "IA with no transitions",
			tier:     "IA",
			expected: "IA",
		},
		{
			name:        "with transitions",
			tier:        "STANDARD",
			transitions: []model.StorageClassTransitRule{{Tier: "IA", AfterDays: 30}},
			expected:    `{"tier":"STANDARD","transitions":[{"tier":"IA","after_days":30}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ti := model.TableInfo{
				StorageClassTier:        tt.tier,
				StorageClassTransitions: tt.transitions,
			}

			result := ti.StorageClassString()
			require.Equal(tt.expected, result)
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
