package network

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/store"
)

// testLogger writes decision lines into the test log so content IDs,
// participating nodes and the basis for each decision are visible.
type testLogger struct{ t *testing.T }

func (l testLogger) Logf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf(format, args...)
}

func newTestNetwork(t *testing.T, n int) (*Network, []*store.Store) {
	t.Helper()
	root := t.TempDir()
	nodes := make([]*store.Store, 0, n)
	for i := 0; i < n; i++ {
		id := nodeName(i)
		s, err := store.New(id, filepath.Join(root, id))
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, s)
	}
	net := New(nodes...)
	net.SetLogger(testLogger{t})
	return net, nodes
}

func nodeName(i int) string {
	return "node-" + string(rune('a'+i))
}

func chunkFor(payload string) chunk.Chunk {
	return chunk.Chunk{ID: chunk.ComputeID([]byte(payload)), Data: []byte(payload)}
}

// countHealthy reports how many target nodes hold a verified copy of id.
func countHealthy(t *testing.T, ctx context.Context, nodes []*store.Store, id chunk.ID) (int, []string) {
	t.Helper()
	healthy := 0
	var names []string
	for _, nd := range nodes {
		ok, err := nd.Has(ctx, id)
		if err == nil && ok {
			healthy++
			names = append(names, nd.ID())
		}
	}
	return healthy, names
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	return true
}
