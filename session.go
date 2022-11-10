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

// Use MEDIUM priority not to disturb regular workloads on the database.
const defaultPriority = pb.RequestOptions_PRIORITY_MEDIUM

type Session struct {
	projectId       string
	instanceId      string
	databaseId      string
	client          *spanner.Client
	adminClient     *adminapi.DatabaseAdminClient
	clientOpts      []option.ClientOption
	defaultPriority pb.RequestOptions_Priority
	tc              *transactionContext
	tcMutex         sync.Mutex // Guard a critical section for transaction.
}

type transactionContext struct {
	statementCounter int64 // Count current statement number in a transaction
	tag              string
	priority         pb.RequestOptions_Priority
	sendHeartbeat    bool // Becomes true only after a user-driven query is executed on the transaction.
	rwTxn            *spanner.ReadWriteStmtBasedTransaction
	roTxn            *spanner.ReadOnlyTransaction
}

func NewSession(projectId string, instanceId string, databaseId string, priority pb.RequestOptions_Priority, opts ...option.ClientOption) (*Session, error) {
	ctx := context.Background()
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

	if priority == pb.RequestOptions_PRIORITY_UNSPECIFIED {
		priority = defaultPriority
	}

	session := &Session{
		projectId:       projectId,
		instanceId:      instanceId,
		databaseId:      databaseId,
		client:          client,
		clientOpts:      opts,
		adminClient:     adminClient,
		defaultPriority: priority,
	}
	go session.startHeartbeat()

	return session, nil
}

// InReadWriteTransaction returns true if the session is running read-write transaction.
func (s *Session) InReadWriteTransaction() bool {
	return s.tc != nil && s.tc.rwTxn != nil
}

// InReadOnlyTransaction returns true if the session is running read-only transaction.
func (s *Session) InReadOnlyTransaction() bool {
	return s.tc != nil && s.tc.roTxn != nil
}

// BeginReadWriteTransaction starts read-write transaction.
func (s *Session) BeginReadWriteTransaction(ctx context.Context, priority pb.RequestOptions_Priority, tag string) error {
	if s.InReadWriteTransaction() {
		return errors.New("read-write transaction is already running")
	}

	// Use session's priority if transaction priority is not set.
	if priority == pb.RequestOptions_PRIORITY_UNSPECIFIED {
		priority = s.defaultPriority
	}

	opts := spanner.TransactionOptions{
		CommitOptions:  spanner.CommitOptions{ReturnCommitStats: true},
		CommitPriority: priority,
		TransactionTag: tag,
	}
	txn, err := spanner.NewReadWriteStmtBasedTransactionWithOptions(ctx, s.client, opts)
	if err != nil {
		return err
	}
	s.tc = &transactionContext{
		statementCounter: 1, // Reset counter
		tag:              tag,
		priority:         priority,
		rwTxn:            txn,
	}
	return nil
}

// CommitReadWriteTransaction commits read-write transaction and returns commit timestamp if successful.
func (s *Session) CommitReadWriteTransaction(ctx context.Context) (spanner.CommitResponse, error) {
	if !s.InReadWriteTransaction() {
		return spanner.CommitResponse{}, errors.New("read-write transaction is not running")
	}

	s.tcMutex.Lock()
	defer s.tcMutex.Unlock()

	resp, err := s.tc.rwTxn.CommitWithReturnResp(ctx)
	s.tc = nil
	return resp, err
}

// RollbackReadWriteTransaction rollbacks read-write transaction.
func (s *Session) RollbackReadWriteTransaction(ctx context.Context) error {
	if !s.InReadWriteTransaction() {
		return errors.New("read-write transaction is not running")
	}

	s.tcMutex.Lock()
	defer s.tcMutex.Unlock()

	s.tc.rwTxn.Rollback(ctx)
	s.tc = nil
	return nil
}

// BeginReadOnlyTransaction starts read-only transaction and returns the snapshot timestamp for the transaction if successful.
func (s *Session) BeginReadOnlyTransaction(ctx context.Context, typ timestampBoundType, staleness time.Duration, timestamp time.Time, priority pb.RequestOptions_Priority, tag string) (time.Time, error) {
	if s.InReadOnlyTransaction() {
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

	// Use session's priority if transaction priority is not set.
	if priority == pb.RequestOptions_PRIORITY_UNSPECIFIED {
		priority = s.defaultPriority
	}

	// Because google-cloud-go/spanner defers calling BeginTransaction RPC until an actual query is run,
	// we explicitly run a "SELECT 1" query so that we can determine the timestamp of read-only transaction.
	opts := spanner.QueryOptions{Priority: priority}
	if err := txn.QueryWithOptions(ctx, spanner.NewStatement("SELECT 1"), opts).Do(func(r *spanner.Row) error {
		return nil
	}); err != nil {
		return time.Time{}, err
	}

	s.tc = &transactionContext{
		statementCounter: 1, // Reset counter
		tag:              tag,
		priority:         priority,
		roTxn:            txn,
	}

	return txn.Timestamp()
}

// CloseReadOnlyTransaction closes a running read-only transaction.
func (s *Session) CloseReadOnlyTransaction() error {
	if !s.InReadOnlyTransaction() {
		return errors.New("read-only transaction is not running")
	}

	s.tc.roTxn.Close()
	s.tc = nil
	return nil
}

// RunQueryWithStats executes a statement with stats either on the running transaction or on the temporal read-only transaction.
// It returns row iterator and read-only transaction if the statement was executed on the read-only transaction.
func (s *Session) RunQueryWithStats(ctx context.Context, stmt spanner.Statement) (*spanner.RowIterator, *spanner.ReadOnlyTransaction) {
	mode := pb.ExecuteSqlRequest_PROFILE
	opts := spanner.QueryOptions{
		Mode:     &mode,
		Priority: s.currentPriority(),
	}
	return s.runQueryWithOptions(ctx, stmt, opts)
}

// RunQuery executes a statement either on the running transaction or on the temporal read-only transaction.
// It returns row iterator and read-only transaction if the statement was executed on the read-only transaction.
func (s *Session) RunQuery(ctx context.Context, stmt spanner.Statement) (*spanner.RowIterator, *spanner.ReadOnlyTransaction) {
	opts := spanner.QueryOptions{
		Priority: s.currentPriority(),
	}
	return s.runQueryWithOptions(ctx, stmt, opts)
}

// RunAnalyzeQuery analyzes a statement either on the running transaction or on the temporal read-only transaction.
func (s *Session) RunAnalyzeQuery(ctx context.Context, stmt spanner.Statement) (*pb.QueryPlan, error) {
	mode := pb.ExecuteSqlRequest_PLAN
	opts := spanner.QueryOptions{
		Mode:     &mode,
		Priority: s.currentPriority(),
	}
	iter, _ := s.runQueryWithOptions(ctx, stmt, opts)

	// Need to read rows from iterator to get the query plan.
	iter.Do(func(r *spanner.Row) error {
		return nil
	})
	if iter.QueryPlan == nil {
		return nil, errors.New("query plan unavailable")
	}
	return iter.QueryPlan, nil
}

func (s *Session) runQueryWithOptions(ctx context.Context, stmt spanner.Statement, opts spanner.QueryOptions) (*spanner.RowIterator, *spanner.ReadOnlyTransaction) {
	if s.InReadWriteTransaction() {
		opts.RequestTag = generateRequestTag(s.tc, stmt)
		iter := s.tc.rwTxn.QueryWithOptions(ctx, stmt, opts)
		s.tc.sendHeartbeat = true
		return iter, nil
	}
	if s.InReadOnlyTransaction() {
		opts.RequestTag = generateRequestTag(s.tc, stmt)
		return s.tc.roTxn.QueryWithOptions(ctx, stmt, opts), s.tc.roTxn
	}

	txn := s.client.Single()
	return txn.QueryWithOptions(ctx, stmt, opts), txn
}

// RunUpdate executes a DML statement on the running read-write transaction.
// It returns error if there is no running read-write transaction.
func (s *Session) RunUpdate(ctx context.Context, stmt spanner.Statement) (int64, error) {
	if !s.InReadWriteTransaction() {
		return 0, errors.New("read-write transaction is not running")
	}

	opts := spanner.QueryOptions{
		Priority:   s.currentPriority(),
		RequestTag: generateRequestTag(s.tc, stmt),
	}
	rowCount, err := s.tc.rwTxn.UpdateWithOptions(ctx, stmt, opts)
	s.tc.sendHeartbeat = true
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	stmt := spanner.NewStatement("SELECT 1")
	iter := s.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: s.currentPriority()})
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
	ctx := context.Background()
	c, err := spanner.NewClientWithConfig(ctx, s.DatabasePath(), clientConfig, s.clientOpts...)
	if err != nil {
		return err
	}
	s.client.Close()
	s.client = c
	return nil
}

func (s *Session) currentPriority() pb.RequestOptions_Priority {
	if s.tc != nil {
		return s.tc.priority
	}
	return s.defaultPriority
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
			s.tcMutex.Lock()
			if s.tc != nil && s.tc.rwTxn != nil && s.tc.sendHeartbeat {
				heartbeat(s.tc.rwTxn, s.currentPriority())
			}
			s.tcMutex.Unlock()
		}
	}
}

func heartbeat(txn *spanner.ReadWriteStmtBasedTransaction, priority pb.RequestOptions_Priority) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	iter := txn.QueryWithOptions(ctx, spanner.NewStatement("SELECT 1"), spanner.QueryOptions{Priority: priority})
	defer iter.Stop()
	_, err := iter.Next()
	return err
}

func generateRequestTag(tc *transactionContext, stmt spanner.Statement) string {
	return tc.tag + "_" + fmt.Sprintf("%d", tc.statementCounter) + "_" + stmt.SQL
}
