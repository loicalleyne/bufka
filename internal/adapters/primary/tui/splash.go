package tui

import (
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// bufkaLogo is the ASCII-art wordmark shown on the loading splash screen.
const bufkaLogo = `
 ____        __ _  __
| __ ) _   _ / _| |/ /__ _
|  _ \| | | | |_| ' // _  |
| |_) | |_| |  _| . \ (_| |
|____/ \__,_|_| |_|\_\__,_|
`

// bufkaBigLogo is the large block-letter wordmark used on the splash screen.
const bufkaBigLogo = `
██████╗ ██╗   ██╗███████╗██╗  ██╗ █████╗
██╔══██╗██║   ██║██╔════╝██║ ██╔╝██╔══██╗
██████╔╝██║   ██║█████╗  █████╔╝ ███████║
██╔══██╗██║   ██║██╔══╝  ██╔═██╗ ██╔══██║
██████╔╝╚██████╔╝██║     ██║  ██╗██║  ██║
╚═════╝  ╚═════╝ ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝
`

// bufkaBigLogoWidth is the display width of bufkaBigLogo's widest line.
const bufkaBigLogoWidth = 42

// splashMinDuration is how long the splash is shown at minimum before the
// main menu appears, even if topics load sooner.
var splashMinDuration = 2 * time.Second

// splashDoneMsg fires once splashMinDuration has elapsed.
type splashDoneMsg struct{}

func splashTimerCmd() tea.Cmd {
	return tea.Tick(splashMinDuration, func(time.Time) tea.Msg { return splashDoneMsg{} })
}

var (
	splashLogoStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("63"))

	splashSubtitleStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("245"))
)

// splashModel renders the loading splash screen shown while the initial
// topic list loads, so the TUI never opens on a blank screen.
type splashModel struct {
	spinner spinner.Model
	width   int
	height  int
}

func newSplashModel() splashModel {
	s := spinner.New()
	s.Spinner = spinner.Dot
	return splashModel{spinner: s}
}

func (m splashModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m splashModel) resize(w, h int) splashModel {
	m.width, m.height = w, h
	return m
}

func (m splashModel) update(msg tea.Msg) (splashModel, tea.Cmd) {
	if _, ok := msg.(spinner.TickMsg); ok {
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m splashModel) View() string {
	art := bufkaBigLogo
	if m.width > 0 && m.width < bufkaBigLogoWidth {
		art = bufkaLogo // terminal too narrow for the big wordmark
	}
	logo := splashLogoStyle.Render(art)
	line := m.spinner.View() + " loading topics..."
	body := lipgloss.JoinVertical(lipgloss.Center,
		logo,
		"",
		splashSubtitleStyle.Render("bufka — Kafka explorer"),
		"",
		line,
	)
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, body)
	}
	return body
}
