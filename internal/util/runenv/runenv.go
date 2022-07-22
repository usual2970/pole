package runenv

import (
	"errors"
	"os"
	"strings"
)

type REnv = string

const (
	DefaultREnv      = Dev
	Dev         REnv = "dev"
	Test        REnv = "test"
	Prod        REnv = "prod"
)

var (
	runEnvKey = "POLE_ENV"
)

// Is reports whether the server is running in its env configuration
func Is(env REnv) bool {
	return strings.HasSuffix(GetRunEnv(), strings.ToLower(env))
}

func Not(env REnv) bool {
	return !Is(env)
}

// IsDev reports whether the server is running in its development configuration
func IsDev() bool {
	return Is(Dev)
}

// IsTest reports whether the server is running in its testing configuration
func IsTest() bool {
	return Is(Test)
}

// IsProd reports whether the server is running in its production configuration
func IsProd() bool {
	return Is(Prod)
}

// Gets the current runtime environment
func GetRunEnv() (e REnv) {
	if e = os.Getenv(runEnvKey); e == "" {
		// Returns a specified default value (Dev) if an empty or invalid value is detected.
		e = DefaultREnv
	}
	return strings.ToLower(e)
}

// Gets the key of the runtime environment
func GetRunEnvKey() string {
	return runEnvKey
}

// Sets the key of the runtime environment
func SetRunEnvKey(key string) error {
	if key == "" {
		return errors.New("[runEnv] RunEnvKey cannot be empty")
	}
	runEnvKey = key
	return nil
}
