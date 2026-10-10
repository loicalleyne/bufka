package tui

import (
	"fmt"
	"os/exec"
	"runtime"

	tea "charm.land/bubbletea/v2"
)

// browserCommand returns the OS command that opens url in the default
// browser.
func browserCommand(goos, url string) (string, []string, error) {
	switch goos {
	case "darwin":
		return "open", []string{url}, nil
	case "windows":
		return "rundll32", []string{"url.dll,FileProtocolHandler", url}, nil
	case "linux", "freebsd", "openbsd", "netbsd":
		return "xdg-open", []string{url}, nil
	}
	return "", nil, fmt.Errorf("opening a browser is not supported on %s", goos)
}

// openBrowser is replaced in tests.
var openBrowser = func(url string) error {
	name, args, err := browserCommand(runtime.GOOS, url)
	if err != nil {
		return err
	}
	// Start, not Run: the browser must not block or inherit the TUI's
	// terminal; Wait in the background so the child is reaped.
	cmd := exec.Command(name, args...)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("open browser: %w", err)
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

// browserOpenedMsg reports the outcome of opening url.
type browserOpenedMsg struct {
	url string
	err error
}

func openBrowserCmd(url string) tea.Cmd {
	return func() tea.Msg { return browserOpenedMsg{url: url, err: openBrowser(url)} }
}
