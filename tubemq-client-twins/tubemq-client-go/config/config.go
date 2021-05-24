package config

import (
	"time"
)

type Config struct {
	Net struct {
		// How long to wait for a response.
		ReadTimeout time.Duration
	}
}
