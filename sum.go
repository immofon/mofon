package mofon

import (
	"crypto/sha256"
	"fmt"
)

func SHA256(data string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(data)))
}
