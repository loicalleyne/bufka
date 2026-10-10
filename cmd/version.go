/*
Copyright © 2026
*/
package cmd

import (
	"fmt"

	"github.com/loicalleyne/bufka/pkg/version"
	"github.com/spf13/cobra"
)

var (
	simple     bool
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Long:  `Display application version, build date, and commit information.`,
		Run: func(cmd *cobra.Command, args []string) {
			simple, _ := cmd.Flags().GetBool("simple")
			if simple {
				fmt.Printf("%s", version.CurrentVersion().Version)
				return
			}

			version.Splash()
		},
	}
)

func init() {
	rootCmd.AddCommand(versionCmd)

	versionCmd.Flags().BoolP("simple", "s", false, "Prints only the version, useful for scripting")
}
