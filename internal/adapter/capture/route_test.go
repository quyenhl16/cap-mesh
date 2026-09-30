package capture

import "testing"

func TestParseRouteInterface(t *testing.T) {
	iface, err := parseRouteInterface("10.244.1.7 dev cali123 scope link src 10.244.1.1")
	if err != nil {
		t.Fatal(err)
	}
	if iface != "cali123" {
		t.Fatalf("interface = %q", iface)
	}
}
