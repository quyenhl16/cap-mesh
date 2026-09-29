package main

import "testing"

func TestInterfaceMappingsSet(t *testing.T) {
	var mappings interfaceMappings
	for _, value := range []string{"management=ens160", "DATA-East=ens192", "a=eth0"} {
		if err := mappings.Set(value); err != nil {
			t.Fatal(err)
		}
	}
	want := map[string]string{"management": "ens160", "data-east": "ens192", "A": "eth0"}
	for alias, physical := range want {
		if mappings[alias] != physical {
			t.Errorf("mapping %q = %q, want %q", alias, mappings[alias], physical)
		}
	}
}

func TestInterfaceMappingsSetRejectsInvalidMapping(t *testing.T) {
	var mappings interfaceMappings
	for _, value := range []string{"management", "bad_alias=eth0", "management="} {
		if err := mappings.Set(value); err == nil {
			t.Errorf("Set(%q) unexpectedly succeeded", value)
		}
	}
}

func TestInterfaceMappingsSetEnforcesLimit(t *testing.T) {
	var mappings interfaceMappings
	for _, value := range []string{"one=e1", "two=e2", "three=e3", "four=e4", "five=e5", "six=e6", "seven=e7", "eight=e8", "nine=e9", "ten=e10"} {
		if err := mappings.Set(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := mappings.Set("eleven=e11"); err == nil {
		t.Fatal("expected eleventh mapping to fail")
	}
}
