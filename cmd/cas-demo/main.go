// Command cas-demo runs a fully local, self-contained demonstration of the
// content-addressed storage network: it brings up several on-disk nodes,
// writes an object with replication, simulates node outage + data loss,
// and shows background repair converging all nodes back to consistency.
//
// Usage:
//
//	go run ./cmd/cas-demo
//
// All data is written under a fresh temporary directory.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/xingjianluo/content-addressed-storage-go/pkg/chunk"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/network"
	"github.com/xingjianluo/content-addressed-storage-go/pkg/store"
)

type stdLogger struct{}

func (stdLogger) Logf(format string, args ...any) {
	log.Printf(format, args...)
}

func main() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "cas-demo-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(root)

	const nodeCount, rf = 5, 3
	var nodes []*store.Store
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%c", 'a'+i)
		s, err := store.New(id, filepath.Join(root, id))
		if err != nil {
			log.Fatal(err)
		}
		nodes = append(nodes, s)
	}
	net := network.New(nodes...)
	net.SetLogger(stdLogger{})
	opt := network.Options{Replication: rf, ChunkSize: 64}

	// 1) Write an object spanning multiple chunks.
	payload := []byte("content-addressed storage demo: ")
	payload = append(payload, make([]byte, 200)...)
	for i := 64; i < len(payload); i++ {
		payload[i] = byte(i % 251)
	}
	manifestID, man, err := net.PutObject(ctx, "demo.bin", payload, opt)
	if err != nil {
		log.Fatalf("put object: %v", err)
	}
	fmt.Printf("WROTE manifest=%s version=%d chunks=%d\n", manifestID, man.Version, len(man.Entries))

	// 2) Read it back and compare.
	got, _, err := net.GetObject(ctx, manifestID, opt)
	if err != nil || string(got) != string(payload) {
		log.Fatalf("read-back mismatch: %v", err)
	}
	fmt.Println("READ-OK identical bytes reconstructed from verified chunks")

	// 3) Start background repair, then take one replica down and lose data
	//    on another, to be recovered when they return.
	loopCtx, stop := context.WithCancel(ctx)
	loopDone := make(chan struct{})
	go func() {
		_ = net.RepairLoop(loopCtx, 100*time.Millisecond, opt)
		close(loopDone)
	}()

	// Find one replica chunk to damage.
	target := man.Entries[0].ID
	victim := net.PreferredNodes(target, rf)[0]
	if err := victim.Delete(ctx, target); err != nil {
		log.Fatal(err)
	}
	victim.SetAvailable(false)
	fmt.Printf("FAULT node=%s lost chunk=%s and went offline\n", victim.ID(), target)

	time.Sleep(250 * time.Millisecond)
	victim.SetAvailable(true)
	fmt.Printf("RECOVER node=%s is back; background repair running\n", victim.ID())

	// Wait for convergence.
	if err := waitConvergent(ctx, net, target, opt, 3*time.Second); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("CONVERGED chunk=%s restored on all %d preferred nodes\n", target, rf)

	stop()
	<-loopDone
	fmt.Println("DEMO-OK integrity verified, replicas converged")
}

func waitConvergent(ctx context.Context, net *network.Network, id chunk.ID, opt network.Options, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := net.Repair(ctx, opt)
		if err != nil {
			return err
		}
		for _, r := range res {
			if r.ID == id && r.Healthy >= opt.Replication {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errors.New("convergence timeout")
}
