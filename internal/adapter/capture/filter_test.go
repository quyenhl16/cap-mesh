package capture

import (
	"context"
	"reflect"
	"testing"

	"golang.org/x/net/bpf"
)

func TestParseDecimalBPF(t *testing.T) {
	got, err := parseDecimalBPF("4\n40 0 0 12\n21 0 1 2048\n6 0 0 256\n6 0 0 0\n")
	if err != nil {
		t.Fatal(err)
	}
	want := []bpf.RawInstruction{
		{Op: 40, K: 12},
		{Op: 21, Jf: 1, K: 2048},
		{Op: 6, K: 256},
		{Op: 6},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("instructions = %#v, want %#v", got, want)
	}
}

func TestParseDecimalBPFRejectsCountMismatch(t *testing.T) {
	if _, err := parseDecimalBPF("2\n6 0 0 256\n"); err == nil {
		t.Fatal("instruction count mismatch unexpectedly accepted")
	}
}

func TestEmptyFilterCompilesToSnaplenReturn(t *testing.T) {
	compiler := NewDumpcapFilterCompiler("binary-that-must-not-run")
	got, err := compiler.Compile(context.Background(), "eth0", "", 512)
	if err != nil {
		t.Fatal(err)
	}
	want := []bpf.RawInstruction{{Op: 0x06, K: 512}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("instructions = %#v, want %#v", got, want)
	}
}
