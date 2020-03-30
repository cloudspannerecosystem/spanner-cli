package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type TxnFinishResponse struct {
	Err             error
	CommitTimestamp time.Time
}

type Session struct {
	ctx         context.Context
	projectId   string
	instanceId  string
	databaseId  string
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient

	// for read-write transaction
	rwTxn         *spanner.ReadWriteTransaction
	txnFinished   chan TxnFinishResponse
	committedChan chan bool

	// for read-only transaction
	roTxn *spanner.ReadOnlyTransaction
}

func NewSession(ctx context.Context, projectId string, instanceId string, databaseId string, clientConfig spanner.ClientConfig, opts ...option.ClientOption) (*Session, error) {
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	client, err := spanner.NewClientWithConfig(ctx, dbPath, clientConfig, opts...)
	if err != nil {
		return nil, err
	}

	if emulatorAddr := os.Getenv("SPANNER_EMULATOR_HOST"); emulatorAddr != "" {
		emulatorOpts := []option.ClientOption{
			option.WithEndpoint(emulatorAddr),
			option.WithGRPCDialOption(grpc.WithInsecure()),
			option.WithoutAuthentication(),
		}
		opts = append(opts, emulatorOpts...)
	}

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &Session{
		ctx:           ctx,
		projectId:     projectId,
		instanceId:    instanceId,
		databaseId:    databaseId,
		client:        client,
		adminClient:   adminClient,
		txnFinished:   make(chan TxnFinishResponse),
		committedChan: make(chan bool),
	}, nil
}

func (s *Session) StartRwTxn(ctx context.Context, txn *spanner.ReadWriteTransaction) func() {
	oldCtx := s.ctx
	s.ctx = ctx
	s.rwTxn = txn

	heartbeat := s.StartHeartbeat()

	finish := func() {
		close(heartbeat)
		s.ctx = oldCtx
		s.rwTxn = nil
	}

	return finish
}

func (s *Session) StartRoTxn(txn *spanner.ReadOnlyTransaction) {
	s.roTxn = txn
}

func (s *Session) FinishRoTxn() {
	s.roTxn.Close()
	s.roTxn = nil
}

func (s *Session) InRwTxn() bool {
	return s.rwTxn != nil
}

func (s *Session) InRoTxn() bool {
	return s.roTxn != nil
}

func (s *Session) Close() {
	s.client.Close()
	s.adminClient.Close()
}

func (s *Session) DatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.projectId, s.instanceId, s.databaseId)
}

func (s *Session) InstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", s.projectId, s.instanceId)
}

func (s *Session) DatabaseExists() (bool, error) {
	// For users who don't have `spanner.databases.get` IAM permission,
	// check database existence by running an actual query.
	// cf. https://github.com/cloudspannerecosystem/spanner-cli/issues/10
	stmt := spanner.NewStatement("SELECT 1")
	iter := s.client.Single().Query(s.ctx, stmt)
	defer iter.Stop()

	_, err := iter.Next()
	if err == nil {
		return true, nil
	}
	switch spanner.ErrCode(err) {
	case codes.NotFound:
		return false, nil
	case codes.InvalidArgument:
		return false, nil
	default:
		return false, fmt.Errorf("checking database existence failed: %v", err)
	}
}

// Start heartbeat for read-write transaction.
//
// If no reads or DMLs happen within 10 seconds, the rw-transaction is considered idle at Cloud Spanner server.
// This "SELECT 1" query prevents the transaction from being considered idle.
// cf. https://godoc.org/cloud.google.com/go/spanner#hdr-Idle_transactions
func (s *Session) StartHeartbeat() chan bool {
	interval := time.NewTicker(5 * time.Second)
	timeout := time.NewTimer(600 * time.Second)
	stop := make(chan bool)
	go func() {
		defer interval.Stop()
		defer timeout.Stop()

		for {
			select {
			case <-interval.C:
				if err := heartbeat(s.ctx, s.rwTxn); err != nil {
					return
				}
			case <-timeout.C:
				return
			case <-stop:
				return
			}
		}
	}()
	return stop
}

func heartbeat(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
	iter := txn.Query(ctx, spanner.NewStatement("SELECT 1"))
	defer iter.Stop()
	_, err := iter.Next()
	return err
}
