package v2raygrpclite

import (
	"os"
	"testing"

	"github.com/sagernet/sing-box/log"
)

func TestMain(m *testing.M) {
	prev := log.StdLogger()
	log.SetStdLogger(log.NewNOPFactory().Logger())
	code := m.Run()
	log.SetStdLogger(prev)
	os.Exit(code)
}
