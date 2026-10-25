package notasap_test

import (
	"os"
	"testing"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Run(m))
}

const (
	kindA notasap.Kind = 1
	kindB notasap.Kind = 2
)

type payloadA struct {
	Msg string `json:"msg"`
	N   int    `json:"n"`
}

func (payloadA) Kind() notasap.Kind { return kindA }

type payloadB struct {
	Msg string `json:"msg"`
}

func (payloadB) Kind() notasap.Kind { return kindB }

type unregisteredPayload struct{}

func (unregisteredPayload) Kind() notasap.Kind { return 99 }
