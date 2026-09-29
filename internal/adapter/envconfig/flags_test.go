package envconfig

import (
	"flag"
	"reflect"
	"testing"
)

type listValue []string

func (v *listValue) String() string { return "" }
func (v *listValue) Set(value string) error {
	*v = append(*v, value)
	return nil
}

func TestApplyUsesEnvironmentAsFlagDefault(t *testing.T) {
	t.Setenv("TEST_LISTEN", ":9000")
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	listen := flags.String("listen", ":8000", "")
	if err := Apply(flags, map[string]string{"TEST_LISTEN": "listen"}); err != nil {
		t.Fatal(err)
	}
	if err := flags.Parse([]string{"--listen=:9100"}); err != nil {
		t.Fatal(err)
	}
	if *listen != ":9100" {
		t.Fatalf("listen = %q, want CLI override :9100", *listen)
	}
}

func TestApplyFallsBackWhenEnvironmentIsMissing(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	listen := flags.String("listen", ":8000", "")
	if err := Apply(flags, map[string]string{"TEST_MISSING_VALUE": "listen"}); err != nil {
		t.Fatal(err)
	}
	if *listen != ":8000" {
		t.Fatalf("listen = %q, want built-in default :8000", *listen)
	}
}

func TestApplyList(t *testing.T) {
	t.Setenv("TEST_INTERFACES", "management=eth0, data=eth1")
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	var values listValue
	flags.Var(&values, "interface", "")
	if err := ApplyList(flags, "TEST_INTERFACES", "interface"); err != nil {
		t.Fatal(err)
	}
	want := listValue{"management=eth0", "data=eth1"}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("interfaces = %#v, want %#v", values, want)
	}
}
