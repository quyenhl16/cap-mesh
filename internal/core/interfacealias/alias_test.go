package interfacealias

import "testing"

func TestNormalize(t *testing.T) {
	tests := map[string]string{
		"management": "management",
		"DATA-East":  "data-east",
		" A ":        "A",
	}
	for input, want := range tests {
		got, err := Normalize(input)
		if err != nil {
			t.Fatalf("Normalize(%q): %v", input, err)
		}
		if got != want {
			t.Errorf("Normalize(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestNormalizeRejectsInvalidAliases(t *testing.T) {
	for _, input := range []string{"", "data_plane", "-data", "data-", "data/one", "this-alias-is-longer-than-fifty-three-characters-and-invalid"} {
		if _, err := Normalize(input); err == nil {
			t.Errorf("Normalize(%q) unexpectedly succeeded", input)
		}
	}
}
