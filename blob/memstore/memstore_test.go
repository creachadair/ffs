package memstore_test

import (
	"testing"

	"bitbucket.org/creachadair/ffs/blob/memstore"
	"bitbucket.org/creachadair/ffs/blob/storetest"
)

func TestStore(t *testing.T) {
	m := memstore.New()
	storetest.Run(t, m)
}
