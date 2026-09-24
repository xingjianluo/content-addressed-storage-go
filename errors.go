// Package cas 实现一套仅依赖本地文件系统与内存协调的内容寻址存储网络，
// 用于验证内容完整性、多副本一致性与故障恢复。
package cas

import (
	"errors"
	"fmt"
)

// FailureKind 是稳定的错误分类标识，测试与日志均以此作为判定依据，
// 不依赖任何人类可读文案。
type FailureKind string

const (
	// KindMissing 内容在目标节点上不存在。
	KindMissing FailureKind = "missing"
	// KindCorrupted 内容存在但字节与其内容标识不一致（数据损坏）。
	KindCorrupted FailureKind = "corrupted"
	// KindIDMismatch 请求的标识与内容/清单实际计算出的标识不一致。
	KindIDMismatch FailureKind = "id_mismatch"
	// KindManifestTooNew 清单版本高于当前软件支持的最高版本。
	KindManifestTooNew FailureKind = "manifest_too_new"
	// KindUnknownField 清单包含按兼容规则不允许忽略的未知字段。
	KindUnknownField FailureKind = "unknown_field"
	// KindQuorum 可用节点数不足以满足读/写法定数量。
	KindQuorum FailureKind = "quorum_unavailable"
	// KindNoHealthyReplica 所有副本均缺失或损坏，无法提供有效数据。
	KindNoHealthyReplica FailureKind = "no_healthy_replica"
)

// 哨兵错误，便于 errors.Is 判定；每个分类一一对应。
var (
	ErrMissingContent    = errors.New("cas: content is missing")
	ErrContentCorrupted  = errors.New("cas: content is corrupted")
	ErrIDMismatch        = errors.New("cas: content id does not match")
	ErrManifestTooNew    = errors.New("cas: manifest version is newer than supported")
	ErrUnknownField      = errors.New("cas: manifest contains unsupported unknown field")
	ErrQuorumUnavailable = errors.New("cas: read/write quorum cannot be reached")
	ErrNoHealthyReplica  = errors.New("cas: no healthy replica available")
)

// sentinelFor 把分类映射到哨兵错误。
func sentinelFor(kind FailureKind) error {
	switch kind {
	case KindMissing:
		return ErrMissingContent
	case KindCorrupted:
		return ErrContentCorrupted
	case KindIDMismatch:
		return ErrIDMismatch
	case KindManifestTooNew:
		return ErrManifestTooNew
	case KindUnknownField:
		return ErrUnknownField
	case KindQuorum:
		return ErrQuorumUnavailable
	case KindNoHealthyReplica:
		return ErrNoHealthyReplica
	default:
		return nil
	}
}

// Failure 在哨兵错误之上附带稳定的分类元数据：
// 失败类别、涉及的内容标识、涉及节点与人类可读的判定细节。
type Failure struct {
	Kind   FailureKind
	ID     ContentID
	Node   string
	Detail string
	Err    error
}

func (f *Failure) Error() string {
	scope := ""
	if f.Node != "" {
		scope = " node=" + f.Node
	}
	id := ""
	if f.ID != "" {
		id = " id=" + string(f.ID)
	}
	return fmt.Sprintf("cas: %s%s%s: %s", f.Kind, id, scope, f.Detail)
}

func (f *Failure) Unwrap() error { return f.Err }

// fail 构造一个带分类的失败，并保证可通过 errors.Is 匹配到对应哨兵。
func fail(kind FailureKind, id ContentID, node, detail string, cause error) *Failure {
	s := sentinelFor(kind)
	if cause != nil {
		s = fmt.Errorf("%w: %v", s, cause)
	}
	return &Failure{Kind: kind, ID: id, Node: node, Detail: detail, Err: s}
}

// IsKind 判定错误是否属于指定失败类别（分类稳定，不依赖错误文案）。
func IsKind(err error, kind FailureKind) bool {
	var f *Failure
	if errors.As(err, &f) {
		return f.Kind == kind
	}
	return false
}
