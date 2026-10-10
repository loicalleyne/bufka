package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/driverinstall"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/spf13/cobra"
)

var (
	setupDriverVersion string
	setupForce         bool
)

// setupCmd installs the DuckDB ADBC driver bufka needs.
var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Install the DuckDB driver bufka needs (run once after installing bufka)",
	Long: `Install the DuckDB ADBC driver from the dbc driver registry into your user
driver directory, where bufka finds it. Nothing to do when a working driver
is already installed; --force reinstalls.

The version defaults to the one this bufka is tested with (` + driverinstall.PinnedVersion + `). The
driver's signature is verified against Columnar's key before it is kept, as
"dbc install" does. Like the dbc CLI, registry requests carry dbc's anonymous
machine and install identifiers.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()
		in, err := driverinstall.New()
		if err != nil {
			return err
		}
		return runSetup(ctx, cmd.OutOrStdout(), in, driverOK, setupDriverVersion, setupForce)
	},
}

// driverOK reports whether couac can load the DuckDB driver.
func driverOK() error {
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		return err
	}
	return db.Close()
}

// driverInstaller is the part of driverinstall.Installer setup uses.
type driverInstaller interface {
	Location() string
	Current() (driverinstall.Installed, bool)
	Install(ctx context.Context, version string) (driverinstall.Installed, error)
}

func runSetup(ctx context.Context, w io.Writer, in driverInstaller, check func() error, version string, force bool) error {
	cur, installed := in.Current()
	if !force && check() == nil {
		switch {
		case installed && version != "" && cur.Version != version:
			// A different version was asked for: fall through to install it.
		case installed:
			_, _ = fmt.Fprintf(w, "DuckDB driver %s is installed (%s); nothing to do.\n", cur.Version, cur.Library)
			return nil
		default:
			_, _ = fmt.Fprintln(w, "A DuckDB driver is already available (found outside the dbc user directory); nothing to do.")
			return nil
		}
	}

	want := version
	if want == "" {
		want = driverinstall.PinnedVersion
	}
	_, _ = fmt.Fprintf(w, "Installing DuckDB driver %s into %s ...\n", want, in.Location())
	got, err := in.Install(ctx, version)
	if err != nil {
		return fmt.Errorf("install the DuckDB driver: %w", err)
	}
	_, _ = fmt.Fprintf(w, "Installed and verified DuckDB driver %s (%s).\n", got.Version, got.Library)

	if err := check(); err != nil {
		return fmt.Errorf("the driver was installed but bufka cannot load it: %w", err)
	}
	_, _ = fmt.Fprintln(w, "bufka can load it. Run \"bufka\" to start, or \"bufka paths\" to see where everything is.")
	return nil
}

func init() {
	setupCmd.Flags().StringVar(&setupDriverVersion, "driver-version", "", "DuckDB driver version to install (default "+driverinstall.PinnedVersion+")")
	setupCmd.Flags().BoolVar(&setupForce, "force", false, "reinstall even when a working driver is present")
	rootCmd.AddCommand(setupCmd)
}
