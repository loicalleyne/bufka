package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// runRootWith executes bare "bufka" with args, faking the terminal check and
// the TUI so nothing full-screen starts.
func runRootWith(t *testing.T, tty bool, args ...string) (tuiRan bool, out string, err error) {
	prevTerm, prevTUI := isTerminal, runTUIFunc
	t.Cleanup(func() {
		isTerminal, runTUIFunc = prevTerm, prevTUI
		rootCmd.SetArgs(nil)
		rootCmd.SetOut(nil)
		_ = rootCmd.Flags().Set("count", "50")
	})
	isTerminal = func() bool { return tty }
	runTUIFunc = func(context.Context) error { tuiRan = true; return nil }

	var buf bytes.Buffer
	rootCmd.SetOut(&buf)
	rootCmd.SetArgs(args)
	err = rootCmd.ExecuteContext(context.Background())
	return tuiRan, buf.String(), err
}

func TestBareBufkaOpensTUIInTerminal(t *testing.T) {
	ran, _, err := runRootWith(t, true)
	if err != nil || !ran {
		t.Fatalf("expected TUI, ran=%v err=%v", ran, err)
	}
}

func TestBareBufkaPrintsHelpWithoutTerminal(t *testing.T) {
	ran, out, err := runRootWith(t, false)
	if err != nil || ran {
		t.Fatalf("must not open the TUI without a terminal, ran=%v err=%v", ran, err)
	}
	if !strings.Contains(out, "Available Commands") {
		t.Fatalf("expected help, got %q", out)
	}
}

func TestBareBufkaAcceptsTUIFlags(t *testing.T) {
	ran, _, err := runRootWith(t, true, "--count", "7", "--timeout", "5s")
	if err != nil || !ran || tuiSampleCount != 7 || tuiSampleTimeout != 5*time.Second {
		t.Fatalf("ran=%v err=%v count=%d timeout=%v", ran, err, tuiSampleCount, tuiSampleTimeout)
	}
}

func TestBareBufkaRejectsUnknownCommand(t *testing.T) {
	ran, _, err := runRootWith(t, true, "nosuchcmd")
	if err == nil || ran {
		t.Fatalf("unknown command must error, not open the TUI: ran=%v err=%v", ran, err)
	}
}
