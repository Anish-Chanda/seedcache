package logger

import "testing"

func TestNewAcceptsSupportedLevels(t *testing.T) {
	for _, level := range []string{"trace", "debug", "info", "warn", "error", "fatal", "panic", "disabled"} {
		t.Run(level, func(t *testing.T) {
			if _, err := New(level); err != nil {
				t.Fatalf("New(%q) error = %v", level, err)
			}
		})
	}
}

func TestNewRejectsUnknownLevel(t *testing.T) {
	if _, err := New("verbose"); err == nil {
		t.Fatal("New() succeeded with an unknown log level")
	}
}
