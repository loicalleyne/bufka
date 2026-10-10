/*
Copyright © 2026
*/
package cmd

import (
	"os"
	"os/signal"

	"golang.org/x/term"

	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/pkg/logger"
	"github.com/spf13/cobra"
)

var (
	envFile string
	cfg     *config.Config
)

// rootCmd represents the base command when called without any subcommands.
// Run bare in a terminal it opens the TUI (same as "bufka tui"); otherwise
// (piped, CI, no tty) it prints help so scripts never block on a full-screen UI.
var rootCmd = &cobra.Command{
	Use:   "bufka",
	Short: "Explore Kafka topics, detect their schemas and cache them in DuckDB",
	Long: `bufka explores a Kafka cluster: it lists topics and consumer groups, samples
messages and detects their protobuf/JSON schemas (using .proto import paths and
AsyncAPI specs), estimates topic sizes, and caches decoded messages in DuckDB.

Run without a command in a terminal, bufka opens its terminal UI (the same as
"bufka tui"; see "bufka tui help" for its keys). The other commands below do
the same work non-interactively; "bufka run" starts the HTTP server.`,
	Args: cobra.NoArgs,
	// main prints the returned error; without this it would appear twice.
	SilenceErrors: true,
	RunE:          runRoot,
}

// isTerminal reports whether bufka is attached to an interactive terminal;
// replaced in tests.
var isTerminal = func() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

func runRoot(cmd *cobra.Command, args []string) error {
	if !isTerminal() {
		return cmd.Help()
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()
	return runTUIFunc(ctx)
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)

	// Persistent flags
	rootCmd.PersistentFlags().StringVar(&envFile, "env-file", "", "env file to load first (also $BUFKA_ENV_FILE); none is read by default")
}

// initConfig loads the environment (and --env-file, if given).
func initConfig() {
	var err error
	cfg, err = config.Load(envFile)
	if err != nil {
		logger.Fatal("Failed to load configuration: %v", err)
	}
}

// GetConfig returns the global configuration
func GetConfig() *config.Config {
	return cfg
}
