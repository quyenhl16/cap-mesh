package recording

import "testing"

func TestParseSize(t *testing.T) {
	tests := map[string]int64{
		"0":      0,
		"100MB":  100_000_000,
		"100MiB": 100 * 1024 * 1024,
		"10GiB":  10 * 1024 * 1024 * 1024,
	}
	for input, want := range tests {
		got, err := ParseSize(input)
		if err != nil {
			t.Fatalf("ParseSize(%q): %v", input, err)
		}
		if got != want {
			t.Errorf("ParseSize(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestParseSizeRejectsInvalidValues(t *testing.T) {
	for _, input := range []string{"", "-1MiB", "1.5GiB", "many"} {
		if _, err := ParseSize(input); err == nil {
			t.Errorf("ParseSize(%q) unexpectedly succeeded", input)
		}
	}
}
