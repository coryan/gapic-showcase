// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
)

func newRootCmd(out io.Writer) *cobra.Command {
	var asJSON bool
	root := &cobra.Command{
		Use:           "veneer-conformance",
		Short:         "Language-agnostic conformance runner for handwritten Google Cloud veneers",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(out)
	root.PersistentFlags().BoolVar(&asJSON, "json", false, "Emit JSON conformance report instead of text")
	root.AddCommand(newDatastoreCmd(out, &asJSON))
	return root
}

func runCLI(ctx context.Context, args []string, out io.Writer) error {
	cmd := newRootCmd(out)
	cmd.SetArgs(args)
	return cmd.ExecuteContext(ctx)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := runCLI(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
