package kubernetes

import "testing"

func TestParseInterfaceAnnotations(t *testing.T) {
	got, err := ParseInterfaceAnnotations(map[string]string{
		"capture.capmesh.io/interface.management": " ens160 ",
		"capture.capmesh.io/interface.data-east":  "ens192",
		"capture.capmesh.io/interface-a":          "eth0",
		"unrelated":                               "ignored",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"management": "ens160", "data-east": "ens192", "A": "eth0"}
	if len(got) != len(want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	for alias, physical := range want {
		if got[alias] != physical {
			t.Errorf("mapping %q = %q, want %q", alias, got[alias], physical)
		}
	}
}

func TestParseInterfaceAnnotationsEnforcesLimit(t *testing.T) {
	annotations := make(map[string]string)
	for _, alias := range []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven"} {
		annotations[interfaceAnnotationPrefix+alias] = "eth0"
	}
	if _, err := ParseInterfaceAnnotations(annotations); err == nil {
		t.Fatal("expected too many mappings to fail")
	}
}
