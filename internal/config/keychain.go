package config

import (
	"errors"

	"github.com/zalando/go-keyring"
)

const keychainService = "tm1cli"

// ErrKeychainNotFound is returned when no entry exists for the given ref.
var ErrKeychainNotFound = errors.New("keychain entry not found")

// Overridable for tests.
var (
	keychainSet    = keyring.Set
	keychainGet    = keyring.Get
	keychainDelete = keyring.Delete
)

// SetKeychainPassword stores a password in the OS keychain under the given ref.
func SetKeychainPassword(ref, password string) error {
	return keychainSet(keychainService, ref, password)
}

// GetKeychainPassword retrieves a password from the OS keychain by ref.
// Returns ErrKeychainNotFound if the entry does not exist.
func GetKeychainPassword(ref string) (string, error) {
	pw, err := keychainGet(keychainService, ref)
	if err != nil {
		if errors.Is(err, keyring.ErrNotFound) {
			return "", ErrKeychainNotFound
		}
		return "", err
	}
	return pw, nil
}

// DeleteKeychainPassword removes the entry; missing entries are not an error.
func DeleteKeychainPassword(ref string) error {
	err := keychainDelete(keychainService, ref)
	if err != nil && !errors.Is(err, keyring.ErrNotFound) {
		return err
	}
	return nil
}

// OverrideKeychainSet swaps the keyring.Set implementation for testing.
// Callers should defer the returned restore function.
func OverrideKeychainSet(fn func(service, user, password string) error) (restore func()) {
	old := keychainSet
	keychainSet = fn
	return func() { keychainSet = old }
}

// OverrideKeychainDelete swaps the keyring.Delete implementation for testing.
// Callers should defer the returned restore function.
func OverrideKeychainDelete(fn func(service, user string) error) (restore func()) {
	old := keychainDelete
	keychainDelete = fn
	return func() { keychainDelete = old }
}
