package survey

import (
	"context"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/stretchr/testify/require"
)

// The Caller's OnSurvey dispatch: a registered async op reaches its handler
// (which may reply from its own goroutine — the Ably comet forwarding
// contract), an unknown op stays MethodNotFound, and the built-in sync ops
// are unaffected. Exercised through a real self-targeted node.Survey, which
// invokes the registered OnSurvey handler without needing a broker.
func TestCallerAsyncHandlerDispatch(t *testing.T) {
	node, err := centrifuge.New(centrifuge.Config{})
	require.NoError(t, err)
	require.NoError(t, node.Run())
	defer func() { _ = node.Shutdown(context.Background()) }()

	c := NewCaller(node)
	c.RegisterAsyncHandler("test_async_op", func(e centrifuge.SurveyEvent, cb centrifuge.SurveyCallback) {
		go func() { // async reply, like the comet forwarding handlers
			cb(centrifuge.SurveyReply{Code: 0, Data: append([]byte("echo:"), e.Data...)})
		}()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results, err := node.Survey(ctx, "test_async_op", []byte("hi"), node.ID())
	require.NoError(t, err)
	res, ok := results[node.ID()]
	require.True(t, ok)
	require.EqualValues(t, 0, res.Code)
	require.Equal(t, "echo:hi", string(res.Data))

	// An unregistered op keeps the MethodNotFound contract.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	results, err = node.Survey(ctx2, "no_such_op", nil, node.ID())
	require.NoError(t, err)
	require.EqualValues(t, MethodNotFound, results[node.ID()].Code)
}
