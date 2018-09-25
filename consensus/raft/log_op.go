package raft

import (
	"context"
	"errors"

	"github.com/gxed/opencensus-go/tag"
	"github.com/gxed/opencensus-go/trace"
	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/state"

	consensus "github.com/libp2p/go-libp2p-consensus"
)

// Type of consensus operation
const (
	LogOpPin = iota + 1
	LogOpUnpin
)

// LogOpType expresses the type of a consensus Operation
type LogOpType int

// LogOp represents an operation for the OpLogConsensus system.
// It implements the consensus.Op interface and it is used by the
// Consensus component.
type LogOp struct {
	SpanCtx   trace.SpanContext
	TagCtx    []byte
	Cid       api.PinSerial
	Type      LogOpType
	consensus *Consensus
}

// ApplyTo applies the operation to the State
func (op *LogOp) ApplyTo(cstate consensus.State) (consensus.State, error) {
	tagmap, err := tag.Decode(op.TagCtx)
	if err != nil {
		logger.Error(err)
	}
	ctx := tag.NewContext(context.Background(), tagmap)
	var span *trace.Span
	ctx, span = trace.StartSpanWithRemoteParent(ctx, "consensus/raft/logop/ApplyTo", op.SpanCtx)
	defer span.End()

	state, ok := cstate.(state.State)
	if !ok {
		// Should never be here
		panic("received unexpected state type")
	}

	switch op.Type {
	case LogOpPin:
		err = state.Add(op.Cid.ToPin())
		if err != nil {
			goto ROLLBACK
		}
		// Async, we let the PinTracker take care of any problems
		op.consensus.rpcClient.GoContext(
			ctx,
			"",
			"Cluster",
			"Track",
			op.Cid,
			&struct{}{},
			nil,
		)
	case LogOpUnpin:
		err = state.Rm(op.Cid.ToPin().Cid)
		if err != nil {
			goto ROLLBACK
		}
		// Async, we let the PinTracker take care of any problems
		op.consensus.rpcClient.GoContext(
			ctx,
			"",
			"Cluster",
			"Untrack",
			op.Cid,
			&struct{}{},
			nil,
		)
	default:
		logger.Error("unknown LogOp type. Ignoring")
	}
	return state, nil

ROLLBACK:
	// We failed to apply the operation to the state
	// and therefore we need to request a rollback to the
	// cluster to the previous state. This operation can only be performed
	// by the cluster leader.
	logger.Error("Rollbacks are not implemented")
	return nil, errors.New("a rollback may be necessary. Reason: " + err.Error())
}
