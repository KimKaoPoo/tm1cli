package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// TestCompletionGenerators verifies that each cobra-built-in completion
// generator produces a non-empty, shell-specific script for tm1cli's rootCmd.
// These are the same generators the default `completion <shell>` subcommands
// invoke under the hood — testing them directly avoids cobra's one-shot writer
// capture in InitDefaultCompletionCmd, which makes the subcommand path hard
// to test repeatedly within a single test binary.
func TestCompletionGenerators(t *testing.T) {
	tests := []struct {
		name     string
		gen      func(io.Writer) error
		contains string
	}{
		{"bash", func(w io.Writer) error { return rootCmd.GenBashCompletionV2(w, true) }, "# bash completion V2 for tm1cli"},
		{"zsh", rootCmd.GenZshCompletion, "#compdef tm1cli"},
		{"fish", func(w io.Writer) error { return rootCmd.GenFishCompletion(w, true) }, "# fish completion for tm1cli"},
		{"powershell", rootCmd.GenPowerShellCompletionWithDesc, "Register-ArgumentCompleter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tt.gen(&buf); err != nil {
				t.Fatalf("generator returned error: %v", err)
			}
			out := buf.String()
			if out == "" {
				t.Fatalf("expected non-empty completion output")
			}
			if !strings.Contains(out, tt.contains) {
				t.Errorf("expected output to contain %q; first 300 chars:\n%s", tt.contains, firstN(out, 300))
			}
		})
	}
}

// TestCompletionCommandRegistered verifies cobra's default completion command
// tree is reachable from rootCmd. This catches accidental
// `rootCmd.CompletionOptions.DisableDefaultCmd = true` regressions and ensures
// every documented shell subcommand exists.
func TestCompletionCommandRegistered(t *testing.T) {
	rootCmd.InitDefaultCompletionCmd()

	completionSub, _, err := rootCmd.Find([]string{"completion"})
	if err != nil || completionSub.Name() != "completion" {
		t.Fatalf("completion command not registered on rootCmd: %v", err)
	}

	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		sub, _, err := completionSub.Find([]string{shell})
		if err != nil || sub.Name() != shell {
			t.Errorf("completion %s subcommand not found: %v", shell, err)
		}
	}
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
