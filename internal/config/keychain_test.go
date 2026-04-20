package config

import (
	"errors"
	"testing"

	"github.com/zalando/go-keyring"
)

func TestSetAndGetKeychainPassword_Roundtrip(t *testing.T) {
	keyring.MockInit()
	// Arrange: no pre-state.
	// Act
	if err := SetKeychainPassword("ref-dev", "s3cret"); err != nil {
		t.Fatalf("SetKeychainPassword failed: %v", err)
	}
	got, err := GetKeychainPassword("ref-dev")
	// Assert
	if err != nil {
		t.Fatalf("GetKeychainPassword failed: %v", err)
	}
	if got != "s3cret" {
		t.Errorf("got %q, want %q", got, "s3cret")
	}
}

func TestGetKeychainPassword_NotFoundReturnsSentinel(t *testing.T) {
	keyring.MockInit()
	_, err := GetKeychainPassword("does-not-exist")
	if !errors.Is(err, ErrKeychainNotFound) {
		t.Errorf("err = %v, want ErrKeychainNotFound", err)
	}
}

func TestDeleteKeychainPassword_RemovesEntry(t *testing.T) {
	keyring.MockInit()
	_ = SetKeychainPassword("ref", "x")
	if err := DeleteKeychainPassword("ref"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err := GetKeychainPassword("ref")
	if !errors.Is(err, ErrKeychainNotFound) {
		t.Errorf("err = %v, want ErrKeychainNotFound", err)
	}
}

func TestDeleteKeychainPassword_NonExistentIsNotError(t *testing.T) {
	keyring.MockInit()
	if err := DeleteKeychainPassword("missing"); err != nil {
		t.Errorf("Delete on missing should not error, got %v", err)
	}
}
