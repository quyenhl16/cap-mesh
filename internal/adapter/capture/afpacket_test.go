package capture

import (
	"errors"
	"testing"
	"time"
)

func TestParseByteSize(t *testing.T) {
	for input, want := range map[string]int64{
		"4096":  4096,
		"64MiB": 64 << 20,
		"1GiB":  1 << 30,
		"10MB":  10_000_000,
	} {
		got, err := ParseByteSize(input)
		if err != nil {
			t.Fatalf("ParseByteSize(%q): %v", input, err)
		}
		if got != want {
			t.Errorf("ParseByteSize(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestAFPacketConfigValidation(t *testing.T) {
	config := DefaultAFPacketConfig()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	config.RingSize++
	if err := config.Validate(); err == nil {
		t.Fatal("non-divisible ring size unexpectedly accepted")
	}
}

func TestRingBudgetReservationAndRelease(t *testing.T) {
	budget, err := NewRingBudget(128)
	if err != nil {
		t.Fatal(err)
	}
	release, err := budget.acquire(96)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budget.acquire(64); !errors.Is(err, ErrRingBudgetExceeded) {
		t.Fatalf("second reservation error = %v, want ErrRingBudgetExceeded", err)
	}
	release()
	release()
	if used, limit := budget.Usage(); used != 0 || limit != 128 {
		t.Fatalf("budget usage = %d/%d, want 0/128", used, limit)
	}
}

func TestAFPacketConfigRejectsSubMillisecondTimeout(t *testing.T) {
	config := DefaultAFPacketConfig()
	config.BlockTimeout = time.Microsecond
	if err := config.Validate(); err == nil {
		t.Fatal("sub-millisecond block timeout unexpectedly accepted")
	}
}
