package cas

import (
	"fmt"
	"path/filepath"
	"testing"
)

// testCluster 构造 N 个独立目录的本地节点并组装集群。
func testCluster(t *testing.T, n, w, r int) (*Cluster, []*Node, string) {
	t.Helper()
	root := t.TempDir()
	nodes := make([]*Node, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		node, err := NewNode(id, filepath.Join(root, id))
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		nodes = append(nodes, node)
	}
	return NewCluster(nodes, Config{ReplicationFactor: n, WriteQuorum: w, ReadQuorum: r}), nodes, root
}

// logVerdict 打印单测日志：内容标识、参与节点与判定依据。
// 所有输出均基于确定的标识与排序后的节点集合，重复运行一致。
func logVerdict(t *testing.T, stage, id string, nodes []string, verdict string, args ...any) {
	t.Helper()
	detail := fmt.Sprintf(verdict, args...)
	t.Logf("[%s] id=%s nodes=%v verdict=%s", stage, id, nodes, detail)
}

// nodeIDs 提取节点标识。
func nodeIDs(nodes []*Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = n.ID()
	}
	return out
}
