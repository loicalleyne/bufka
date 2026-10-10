/*
Copyright © 2026
*/
package version

import (
	"fmt"
	"os"
)

func Splash() {
	v := CurrentVersion()
	extra := ""
	if v.Extra != "" {
		extra = "-" + v.Extra
	}

	fmt.Fprintf(os.Stdout, `
┓     ┏┓ ┓      Version: %d.%d.%d%s
┣┓ ┓┏ ╋  ┃┏ ┏┓  Build: %s
┗┛ ┗┻ ┛  ┛┗ ┗┻  Commit: %s

`, v.Major, v.Minor, v.Patch, extra, v.BuildDate, v.Commit)
}
