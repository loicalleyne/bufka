/*
Copyright © 2026
*/
package cmd

import (
	"fmt"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"github.com/spf13/cobra"
)

// protoCmd groups proto schema utility subcommands.
var protoCmd = &cobra.Command{
	Use:   "proto",
	Short: "Proto schema utility commands",
}

// protoImportPaths are additional directories searched for imports not found under dir.
var protoImportPaths []string

// protoListMessagesCmd prints the fully-qualified names of every message
// declared under a directory of .proto files.
var protoListMessagesCmd = &cobra.Command{
	Use:   "list-messages <dir>",
	Short: "List fully-qualified message names declared under a directory of .proto files",
	Long: `Walk <dir> for .proto files, compile them, and print the fully-qualified
name of every message they declare, one per line, in the format:

  <fully.qualified.Name>	<source.proto>

Imports not found under <dir> are searched for in each --import-path, then in
PROTO_INCLUDE_PATHS. google/protobuf/* and google/type/* are built in.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		extra := protoImportPaths
		if cfg != nil {
			extra = append(append([]string(nil), extra...), cfg.ProtoIncludePaths...)
		}
		messages, err := protoinspect.ListMessageFullNames(args[0], extra...)
		if err != nil {
			return err
		}
		if messages.Len() == 0 {
			fmt.Println("no messages found")
			return nil
		}
		messages.Range(func(fqn, file string) bool {
			fmt.Printf("%s\t%s\n", fqn, file)
			return true
		})
		return nil
	},
}

func init() {
	protoListMessagesCmd.Flags().StringSliceVar(&protoImportPaths, "import-path", nil, "additional directory to search for imports not found under <dir> (repeatable)")
	rootCmd.AddCommand(protoCmd)
	protoCmd.AddCommand(protoListMessagesCmd)
}
