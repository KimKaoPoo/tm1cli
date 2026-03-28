package cmd

import (
	"bytes"
	"io"
	"os"
	"testing"
)

// capturedOutput holds captured stdout and stderr.
type capturedOutput struct {
	Stdout string
	Stderr string
}

// captureStdout captures os.Stdout output during the execution of fn.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create pipe: %v", err)
	}

	origStdout := os.Stdout
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = origStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	r.Close()

	return buf.String()
}

// captureStderr captures os.Stderr output during the execution of fn.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create pipe: %v", err)
	}

	origStderr := os.Stderr
	os.Stderr = w

	fn()

	w.Close()
	os.Stderr = origStderr

	var buf bytes.Buffer
	buf.ReadFrom(r)
	r.Close()

	return buf.String()
}

// captureAll captures both stdout and stderr during the execution of fn.
func captureAll(t *testing.T, fn func()) capturedOutput {
	t.Helper()

	origStdout := os.Stdout
	origStderr := os.Stderr

	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create stdout pipe: %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("cannot create stderr pipe: %v", err)
	}

	os.Stdout = wOut
	os.Stderr = wErr

	fn()

	wOut.Close()
	wErr.Close()
	os.Stdout = origStdout
	os.Stderr = origStderr

	outBytes, _ := io.ReadAll(rOut)
	errBytes, _ := io.ReadAll(rErr)

	return capturedOutput{
		Stdout: string(outBytes),
		Stderr: string(errBytes),
	}
}
