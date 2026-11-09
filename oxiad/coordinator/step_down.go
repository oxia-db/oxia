// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"os"
	"time"

	"github.com/oxia-db/oxia/common/failpoint"
)

const (
	// quorumCheckInterval is how often the leader checks that it reaches the data servers.
	quorumCheckInterval = time.Second

	// minDataServersToStepDown is the number of data servers below which the leader never steps down: with fewer, the
	// failure of a single data server is the loss of the majority, and every coordinator would hand over in turn.
	minDataServersToStepDown = 3

	// releaseGracePeriod is how long the coordinator stays down after it released the lease, before it exits and is
	// restarted. The standbys poll the lease every 2 s and the restarted process campaigns at once: without the wait,
	// it takes the lease back before any standby does.
	releaseGracePeriod = 5 * time.Second
)

// quorumLossTracker tells when the leader has been unable to reach a majority of the data servers for the timeout.
// Only the goroutine of the watcher uses it.
type quorumLossTracker struct {
	timeout time.Duration
	// leaderSince is when the coordinator became the leader. Its controllers need the timeout to reach the data
	// servers: until then, the data servers that have not answered yet are not a loss.
	leaderSince time.Time
	// lostSince is when the majority was first seen unreachable, or zero while it is reachable.
	lostSince time.Time
}

// observe returns true once the majority has been unreachable for the timeout.
func (t *quorumLossTracker) observe(reachable, total int, now time.Time) bool {
	if total < minDataServersToStepDown || reachable > total/2 {
		t.lostSince = time.Time{}
		return false
	}
	if now.Sub(t.leaderSince) < t.timeout {
		return false
	}
	if t.lostSince.IsZero() {
		t.lostSince = now
	}
	return now.Sub(t.lostSince) >= t.timeout
}

// bgQuorumWatcher gives up the leadership when the leader cannot reach a majority of the data servers: it only
// tells that it reaches the metadata store, so a leader cut off from the data servers would keep the lease and
// nobody would replace the shard leaders that it can't reach.
func (s *coordinator) bgQuorumWatcher(timeout time.Duration) {
	tracker := quorumLossTracker{timeout: timeout, leaderSince: time.Now()}
	ticker := time.NewTicker(quorumCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			reachable, total := s.runtime.DataServerReachability()
			if tracker.observe(reachable, total, now) {
				s.stepDown(reachable, total, timeout)
				return
			}
		}
	}
}

// stepDown stops coordinating, releases the lease and exits, so that a coordinator that reaches the data servers
// takes over. The restart of the process is up to the orchestrator, which restarts a process that exits.
func (s *coordinator) stepDown(reachable, total int, timeout time.Duration) {
	s.logger.Error("Cannot reach a majority of the data servers: giving up the leadership",
		"reachable", reachable, "total", total, "timeout", timeout)
	// Stop acting first: the next leader must not meet a coordinator that still elects leaders. Closing the
	// runtime also ends the streams that push the assignments, which makes the data servers cut the clients off the
	// assignments that nobody updates anymore.
	_ = s.reconciler.Close()
	_ = s.runtime.Close()
	// Closing the metadata alone does not close the providers, which release the lease when they close.
	_ = s.metadata.Close()
	_ = s.metadataFactory.Close()
	time.Sleep(releaseGracePeriod)
	if exit, ok := failpoint.Eval[func()]("coordinator.stepDownExit"); ok {
		exit()
		return
	}
	os.Exit(1) // coverage-ignore: terminates the process
}
