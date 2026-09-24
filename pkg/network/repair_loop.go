package network

import (
	"context"
	"time"
)

// RepairLoop runs Repair periodically until ctx is canceled. It is the
// "background repair" mechanism: nodes that return after an outage are
// re-synced from verified copies without any client action. Each pass is
// fully logged through the network logger (IDs, nodes, basis).
//
// It returns nil after ctx is canceled (shutdown is not an error).
func (n *Network) RepairLoop(ctx context.Context, interval time.Duration, opt Options) error {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		results, err := n.Repair(ctx, opt)
		if err != nil {
			n.logger.Logf("repair-loop pass error=%v", err)
			return
		}
		repaired := 0
		for _, r := range results {
			repaired += len(r.Repaired)
		}
		n.logger.Logf("repair-loop pass chunks=%d restored-slots=%d basis=periodic-background-scan",
			len(results), repaired)
	}

	runOnce() // repair promptly on startup, then on every tick
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			runOnce()
		}
	}
}
