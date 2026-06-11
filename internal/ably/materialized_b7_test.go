package ably

// B7: a mutation must commit its materialized state only AFTER the broker
// publish succeeds. Committing first left a phantom version when the
// publish failed — benign on the memory broker (publish failures are
// effectively node-shutdown-only and the in-process memory broker still
// accepts them) but a real runtime path once the broker is Redis. The
// fix splits the mutation into prepareMutation (compute the op, no state
// change) + a commit closure the caller invokes only on publish success.
//
// This store-level test deterministically pins the two-phase contract. An
// end-to-end test that injects a genuine broker-publish failure needs a
// fault-injectable broker (the memory broker cannot fail a publish); it is
// added under D2/D7 against Redis, where a broker hiccup is a real,
// injectable failure.

import (
	"testing"

	"github.com/centrifugal/centrifugo/v6/internal/ably/protocol"
	"github.com/stretchr/testify/require"
)

func TestPrepareMutationNoPhantomBeforeCommit_B7(t *testing.T) {
	t.Parallel()
	store := newMaterializedStore()
	const channel = "ai:b7-store"
	store.create(channel, &protocol.Message{Serial: "s1", Name: "n", Data: "hi", Timestamp: 100})

	op, commit, prob := store.prepareMutation(channel, "s1", protocol.MessageActionAppend, " world", "", nil,
		&protocol.MessageVersion{Serial: "v1"})
	require.Nil(t, prob)
	require.NotNil(t, op)
	require.Equal(t, " world", op.Data, "the op carries the delta")

	// Before commit (a failed publish would skip it): the store is UNCHANGED
	// — no phantom version, the materialized state is the original create.
	state, _ := store.get(channel, "s1")
	require.Equal(t, "hi", state.Data, "no phantom — state unchanged before commit")
	versions, _ := store.versions(channel, "s1")
	require.Len(t, versions, 1, "only the create version exists before commit")

	// After commit (publish succeeded): state applied, version recorded.
	commit()
	state, _ = store.get(channel, "s1")
	require.Equal(t, "hi world", state.Data)
	versions, _ = store.versions(channel, "s1")
	require.Len(t, versions, 2)
}
