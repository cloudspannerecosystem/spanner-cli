package main

import (
	"context"
	"fmt"
	"regexp"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
)

var (
	promptReInTransaction = regexp.MustCompile(`\\t`)
	promptReProjectId     = regexp.MustCompile(`\\p`)
	promptReInstanceId    = regexp.MustCompile(`\\i`)
	promptReDatabaseId    = regexp.MustCompile(`\\d`)
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

func (s *Session) inRwTxn() bool {
	return s.rwTxn != nil
}

func (s *Session) inRoTxn() bool {
	return s.roTxn != nil
}

func (s *Session) finishRwTxn() {
	s.rwTxn = nil
}

func (s *Session) finishRoTxn() {
	s.roTxn.Close()
	s.roTxn = nil
}

func (s *Session) GetDatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.projectId, s.instanceId, s.databaseId)
}

func (s *Session) GetInstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", s.projectId, s.instanceId)
}

func (s *Session) InterpolatePromptVariable(prompt string) string {
	prompt = promptReProjectId.ReplaceAllString(prompt, s.projectId)
	prompt = promptReInstanceId.ReplaceAllString(prompt, s.instanceId)
	prompt = promptReDatabaseId.ReplaceAllString(prompt, s.databaseId)

	if s.inRwTxn() {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "(rw txn)")
	} else if s.inRoTxn() {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "(ro txn)")
	} else {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "")
	}

	return prompt
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
