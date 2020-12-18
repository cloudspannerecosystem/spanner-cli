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
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

type txnFinishResult struct {
	CommitTimestamp time.Time
	Err             error
}

var clientConfig = spanner.ClientConfig{
	SessionPoolConfig: spanner.SessionPoolConfig{
		MaxOpened: 1,
		MinOpened: 1,
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
	rwTxn         *spanner.ReadWriteStmtBasedTransaction
	heartbeatChan chan bool

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

	return &Session{
		ctx:         ctx,
		projectId:   projectId,
		instanceId:  instanceId,
		databaseId:  databaseId,
		client:      client,
		clientOpts:  opts,
		adminClient: adminClient,
	}, nil
}

func (s *Session) StartRwTxn(txn *spanner.ReadWriteStmtBasedTransaction) {
	s.rwTxn = txn
}

func (s *Session) FinishRwTxn() {
	s.rwTxn = nil
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

// StartHeartbeat starts heartbeat for read-write transaction.
//
// If no reads or DMLs happen within 10 seconds, the rw-transaction is considered idle at Cloud Spanner server.
// This "SELECT 1" query prevents the transaction from being considered idle.
// cf. https://godoc.org/cloud.google.com/go/spanner#hdr-Idle_transactions
func (s *Session) StartHeartbeat() {
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
	s.heartbeatChan = stop
}

func (s *Session) StopHeartbeat() {
	close(s.heartbeatChan)
}

func heartbeat(ctx context.Context, txn *spanner.ReadWriteStmtBasedTransaction) error {
	iter := txn.Query(ctx, spanner.NewStatement("SELECT 1"))
	defer iter.Stop()
	_, err := iter.Next()
	return err
}
