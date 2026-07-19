package simulation_test

import (
	"reflect"
	"testing"

	"github.com/Het-Jethva/quorumkv/internal/raft"
	"github.com/Het-Jethva/quorumkv/internal/simulation"
)

func TestThreeNodeElectionElectsExactlyOneLeader(t *testing.T) {
	result, err := simulation.RunElection(42)
	if err != nil {
		t.Fatalf("run election: %v", err)
	}
	if result.Leader == "" {
		t.Fatal("election returned no Leader")
	}
	if result.Term != 1 {
		t.Fatalf("elected Term = %d, want 1", result.Term)
	}

	leaderActions := 0
	for _, step := range result.Trace {
		for _, action := range step.Actions {
			if _, ok := action.(raft.BecameLeader); ok {
				leaderActions++
			}
		}
	}
	if leaderActions != 1 {
		t.Fatalf("BecameLeader actions = %d, want 1", leaderActions)
	}
}

func TestFixedSeedReproducesEventAndActionSequence(t *testing.T) {
	first, err := simulation.RunElection(8675309)
	if err != nil {
		t.Fatalf("run first election: %v", err)
	}
	second, err := simulation.RunElection(8675309)
	if err != nil {
		t.Fatalf("run second election: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different results:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}
