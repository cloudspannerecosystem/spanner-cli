//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"

	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	pb "google.golang.org/genproto/googleapis/spanner/v1"
)

var clientConfig = spanner.ClientConfig{
	SessionPoolConfig: spanner.SessionPoolConfig{
		MinOpened: 1,
		MaxOpened: 10, // FIXME: integration_test requires more than a single session
	},
}

var defaultClientOpts = []option.ClientOption{
	option.WithGRPCConnectionPool(1),
}

type Session struct {
	ctx         context.Context
	projectId   string
	instanceId  string
	databaseId  string
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
	clientOpts  []option.ClientOption

	// for read-write transaction
	rwTxn *spanner.ReadWriteStmtBasedTransaction
	// rwTxn is supposed to be accessed concurrently from transaction handling and heartbeat,
	// so we will use mutex to guard a critical section.
	rwTxnMutex    sync.Mutex
	sendHeartbeat bool

	// for read-only transaction
	roTxn *spanner.ReadOnlyTransaction
}

func NewSession(ctx context.Context, projectId string, instanceId string, databaseId string, opts ...option.ClientOption) (*Session, error) {
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	opts = append(opts, defaultClientOpts...)

	client, err := spanner.NewClientWithConfig(ctx, dbPath, clientConfig, opts...)
	if err != nil {
		return nil, err
	}

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	session := &Session{
		ctx:         ctx,
		projectId:   projectId,
		instanceId:  instanceId,
		databaseId:  databaseId,
		client:      client,
		clientOpts:  opts,
		adminClient: adminClient,
	}
	go session.startHeartbeat()

	return session, nil
}

// InReadWriteTransaction returns true if the session is running read-write transaction.
func (s *Session) InReadWriteTransaction() bool {
	return s.rwTxn != nil
}

// InReadOnlyTransaction returns true if the session is running read-only transaction.
func (s *Session) InReadOnlyTransaction() bool {
	return s.roTxn != nil
}

// BeginReadWriteTransaction starts read-write transaction.
func (s *Session) BeginReadWriteTransaction() error {
	if s.rwTxn != nil {
		return errors.New("read-write transaction is already running")
	}

	txn, err := spanner.NewReadWriteStmtBasedTransactionWithOptions(s.ctx, s.client, spanner.TransactionOptions{CommitOptions: spanner.CommitOptions{ReturnCommitStats: true}})
	if err != nil {
		return err
	}
	s.rwTxn = txn
	return nil
}

// CommitReadWriteTransaction commits read-write transaction and returns commit timestamp if successful.
func (s *Session) CommitReadWriteTransaction() (spanner.CommitResponse, error) {
	if s.rwTxn == nil {
		return spanner.CommitResponse{}, errors.New("read-write transaction is not running")
	}

	s.rwTxnMutex.Lock()
	defer s.rwTxnMutex.Unlock()

	resp, err := s.rwTxn.CommitWithReturnResp(s.ctx)
	s.rwTxn = nil
	s.sendHeartbeat = false
	return resp, err
}

// RollbackReadWriteTransaction rollbacks read-write transaction.
func (s *Session) RollbackReadWriteTransaction() error {
	if s.rwTxn == nil {
		return errors.New("read-write transaction is not running")
	}

	s.rwTxnMutex.Lock()
	defer s.rwTxnMutex.Unlock()

	s.rwTxn.Rollback(s.ctx)
	s.rwTxn = nil
	s.sendHeartbeat = false

	return nil
}

// BeginReadOnlyTransaction starts read-only transaction and returns the snapshot timestamp for the transaction if successful.
func (s *Session) BeginReadOnlyTransaction(typ timestampBoundType, staleness time.Duration, timestamp time.Time) (time.Time, error) {
	if s.roTxn != nil {
		return time.Time{}, errors.New("read-only transaction is already running")
	}

	txn := s.client.ReadOnlyTransaction()
	switch typ {
	case strong:
		txn = txn.WithTimestampBound(spanner.StrongRead())
	case exactStaleness:
		txn = txn.WithTimestampBound(spanner.ExactStaleness(staleness))
	case readTimestamp:
		txn = txn.WithTimestampBound(spanner.ReadTimestamp(timestamp))
	}

	// Because google-cloud-go/spanner defers calling BeginTransaction RPC until an actual query is run,
	// we explicitly run a "SELECT 1" query so that we can determine the timestamp of read-only transaction.
	if err := txn.Query(s.ctx, spanner.NewStatement("SELECT 1")).Do(func(r *spanner.Row) error {
		return nil
	}); err != nil {
		return time.Time{}, err
	}

	s.roTxn = txn

	return txn.Timestamp()
}

// CloseReadOnlyTransaction closes a running read-only transaction.
func (s *Session) CloseReadOnlyTransaction() error {
	if s.roTxn == nil {
		return errors.New("read-only transaction is not running")
	}

	s.roTxn.Close()
	s.roTxn = nil
	return nil
}

// RunQueryWithStats executes a statement with stats either on the running transaction or on the temporal read-only transaction.
// It returns row iterator and read-only transaction if the statement was executed on the read-only transaction.
func (s *Session) RunQueryWithStats(stmt spanner.Statement) (*spanner.RowIterator, *spanner.ReadOnlyTransaction) {
	if s.rwTxn != nil {
		iter := s.rwTxn.QueryWithStats(s.ctx, stmt)
		s.sendHeartbeat = true
		return iter, nil
	}
	if s.roTxn != nil {
		return s.roTxn.QueryWithStats(s.ctx, stmt), s.roTxn
	}

	txn := s.client.Single()
	return txn.QueryWithStats(s.ctx, stmt), txn
}

// RunQuery executes a statement either on the running transaction or on the temporal read-only transaction.
// It returns row iterator and read-only transaction if the statement was executed on the read-only transaction.
func (s *Session) RunQuery(stmt spanner.Statement) (*spanner.RowIterator, *spanner.ReadOnlyTransaction) {
	if s.rwTxn != nil {
		iter := s.rwTxn.Query(s.ctx, stmt)
		s.sendHeartbeat = true
		return iter, nil
	}
	if s.roTxn != nil {
		return s.roTxn.Query(s.ctx, stmt), s.roTxn
	}

	txn := s.client.Single()
	return txn.Query(s.ctx, stmt), txn
}

// RunAnalyzeQuery analyzes a statement either on the running transaction or on the temporal read-only transaction.
func (s *Session) RunAnalyzeQuery(stmt spanner.Statement) (*pb.QueryPlan, error) {
	if s.rwTxn != nil {
		plan, err := s.rwTxn.AnalyzeQuery(s.ctx, stmt)
		s.sendHeartbeat = true
		return plan, err
	}
	if s.roTxn != nil {
		return s.roTxn.AnalyzeQuery(s.ctx, stmt)
	}

	txn := s.client.Single()
	return txn.AnalyzeQuery(s.ctx, stmt)
}

// RunUpdate executes a DML statement on the running read-write transaction.
// It returns error if there is no running read-write transaction.
func (s *Session) RunUpdate(stmt spanner.Statement) (int64, error) {
	if s.rwTxn == nil {
		return 0, errors.New("read-write transaction is not running")
	}

	rowCount, err := s.rwTxn.Update(s.ctx, stmt)
	s.sendHeartbeat = true
	return rowCount, err
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

// RecreateClient closes the current client and creates a new client for the session.
func (s *Session) RecreateClient() error {
	c, err := spanner.NewClientWithConfig(s.ctx, s.DatabasePath(), clientConfig, s.clientOpts...)
	if err != nil {
		return err
	}
	s.client.Close()
	s.client = c
	return nil
}

// startHeartbeat starts heartbeat for read-write transaction.
//
// If no reads or DMLs happen within 10 seconds, the rw-transaction is considered idle at Cloud Spanner server.
// This "SELECT 1" query prevents the transaction from being considered idle.
// cf. https://godoc.org/cloud.google.com/go/spanner#hdr-Idle_transactions
//
// We send an actual heartbeat only if the read-write transaction is active and
// at least one user-initialized SQL query has been executed on the transaction.
// Background: https://github.com/cloudspannerecosystem/spanner-cli/issues/100
func (s *Session) startHeartbeat() {
	interval := time.NewTicker(5 * time.Second)
	defer interval.Stop()

	for {
		select {
		case <-interval.C:
			s.rwTxnMutex.Lock()
			if s.rwTxn != nil && s.sendHeartbeat {
				heartbeat(s.ctx, s.rwTxn)
			}
			s.rwTxnMutex.Unlock()
		}
	}
}

func heartbeat(ctx context.Context, txn *spanner.ReadWriteStmtBasedTransaction) error {
	iter := txn.Query(ctx, spanner.NewStatement("SELECT 1"))
	defer iter.Stop()
	_, err := iter.Next()
	return err
}
