package broker

import (
	"crypto/rand"
	"fmt"
)

type uuid string

func NewId() (uuid, error) {
	var id uuid
	var b [16]byte
  n, err := rand.Read(b[:])
  if err != nil {
    return id, err
  }
	return uuid(fmt.Sprintf("%x", b[:n])), nil
}
