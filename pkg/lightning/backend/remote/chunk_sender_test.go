package remote

import "testing"

func TestParseExpectedChunkIDFromConflictMsg(t *testing.T) {
	tests := []struct {
		name string
		body string
		want uint64
		ok   bool
	}{
		{
			name: "expected-number",
			body: "out-of-order chunk, got 12, expected 34",
			want: 34,
			ok:   true,
		},
		{
			name: "trailing-punctuation",
			body: "out-of-order chunk, got 12, expected 34.\n",
			want: 34,
			ok:   true,
		},
		{
			name: "missing-expected",
			body: "conflict",
			ok:   false,
		},
		{
			name: "expected-not-number",
			body: "out-of-order chunk, got 12, expected NaN",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseExpectedChunkIDFromConflictMsg([]byte(tt.body))
			if ok != tt.ok {
				t.Fatalf("ok=%v, want %v; got=%d body=%q", ok, tt.ok, got, tt.body)
			}
			if got != tt.want {
				t.Fatalf("got=%d, want %d; ok=%v body=%q", got, tt.want, ok, tt.body)
			}
		})
	}
}
