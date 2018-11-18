package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

type Session struct {
	ctx         context.Context
	projectId   string
	instanceId  string
	databaseId  string
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient

	// for read-write transaction
	rwTxn         *spanner.ReadWriteTransaction
	txnFinished   chan error
	committedChan chan bool

	// for read-only transaction
	roTxn *spanner.ReadOnlyTransaction
}

func NewSession(ctx context.Context, projectId string, instanceId string, databaseId string, clientConfig spanner.ClientConfig, clientOptions ...option.ClientOption) (*Session, error) {
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	client, err := spanner.NewClientWithConfig(ctx, dbPath, clientConfig, clientOptions...)
	if err != nil {
		return nil, err
	}

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx, clientOptions...)
	if err != nil {
		return nil, err
	}

	session := &Session{
		ctx:           ctx,
		projectId:     projectId,
		instanceId:    instanceId,
		databaseId:    databaseId,
		client:        client,
		adminClient:   adminClient,
		txnFinished:   make(chan error),
		committedChan: make(chan bool),
	}

	return session, nil
}

func (s *Session) StartRwTxn(ctx context.Context, txn *spanner.ReadWriteTransaction) func() {
	oldCtx := s.ctx
	s.ctx = ctx
	s.rwTxn = txn
	finish := func() {
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

func (s *Session) GetDatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.projectId, s.instanceId, s.databaseId)
}

func (s *Session) GetInstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", s.projectId, s.instanceId)
}

func (s *Session) DatabaseExists() (bool, error) {
	// For users who don't have `spanner.databases.get` IAM permission,
	// check database existence by running an actual query.
	// cf. https://github.com/yfuruyama/spanner-cli/issues/10
	stmt := spanner.NewStatement("SELECT 1")
	iter := s.client.Single().Query(s.ctx, stmt)
	defer iter.Stop()

	_, err := iter.Next()
	if err == nil {
		return true, nil
	} else {
		if code := spanner.ErrCode(err); code == codes.NotFound || code == codes.InvalidArgument {
			return false, nil
		} else {
			return false, fmt.Errorf("Checking database existence failed: %s", err)
		}
	}
}
