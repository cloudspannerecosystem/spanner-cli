package main

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/spannertest"
	"cloud.google.com/go/spanner/spansql"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/testing/protocmp"

	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

func TestRequestPriority(t *testing.T) {
	server := setupTestServer(t)

	var recorder requestRecorder
	unaryInterceptor, streamInterceptor := recordRequestsInterceptors(&recorder)
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(unaryInterceptor),
		grpc.WithStreamInterceptor(streamInterceptor),
	}
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, server.Addr, opts...)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	for _, test := range []struct {
		desc                string
		sessionPriority     pb.RequestOptions_Priority
		transactionPriority pb.RequestOptions_Priority
		want                pb.RequestOptions_Priority
	}{
		{
			desc:                "use default MEDIUM priority",
			sessionPriority:     pb.RequestOptions_PRIORITY_UNSPECIFIED,
			transactionPriority: pb.RequestOptions_PRIORITY_UNSPECIFIED,
			want:                pb.RequestOptions_PRIORITY_MEDIUM,
		},
		{
			desc:                "use session priority",
			sessionPriority:     pb.RequestOptions_PRIORITY_LOW,
			transactionPriority: pb.RequestOptions_PRIORITY_UNSPECIFIED,
			want:                pb.RequestOptions_PRIORITY_LOW,
		},
		{
			desc:                "use transaction priority",
			sessionPriority:     pb.RequestOptions_PRIORITY_UNSPECIFIED,
			transactionPriority: pb.RequestOptions_PRIORITY_HIGH,
			want:                pb.RequestOptions_PRIORITY_HIGH,
		},
		{
			desc:                "transaction priority takes over session priority",
			sessionPriority:     pb.RequestOptions_PRIORITY_HIGH,
			transactionPriority: pb.RequestOptions_PRIORITY_LOW,
			want:                pb.RequestOptions_PRIORITY_LOW,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			defer recorder.flush()

			session, err := NewSession("project", "instance", "database", test.sessionPriority, "role", nil, nil, option.WithGRPCConn(conn))
			if err != nil {
				t.Fatalf("failed to create spanner-cli session: %v", err)
			}

			// Read-Write Transaction.
			if err := session.BeginReadWriteTransaction(ctx, test.transactionPriority, ""); err != nil {
				t.Fatalf("failed to begin read write transaction: %v", err)
			}
			iter, _ := session.RunQuery(ctx, spanner.NewStatement("SELECT * FROM t1"))
			if err := iter.Do(func(r *spanner.Row) error {
				return nil
			}); err != nil {
				t.Fatalf("failed to run query: %v", err)
			}
			if _, _, _, _, err := session.RunUpdate(ctx, spanner.NewStatement("DELETE FROM t1 WHERE Id = 1"), true); err != nil {
				t.Fatalf("failed to run update: %v", err)
			}
			if _, err := session.CommitReadWriteTransaction(ctx); err != nil {
				t.Fatalf("failed to commit: %v", err)
			}

			// Read-Only Transaction.
			if _, err := session.BeginReadOnlyTransaction(ctx, strong, 0, time.Now(), test.transactionPriority, ""); err != nil {
				t.Fatalf("failed to begin read only transaction: %v", err)
			}
			iter, _ = session.RunQueryWithStats(ctx, spanner.NewStatement("SELECT * FROM t1"))
			if err := iter.Do(func(r *spanner.Row) error {
				return nil
			}); err != nil {
				t.Fatalf("failed to run query with stats: %v", err)
			}
			if err := session.CloseReadOnlyTransaction(); err != nil {
				t.Fatalf("failed to close read only transaction: %v", err)
			}

			// Check request priority.
			for _, r := range recorder.requests {
				switch v := r.(type) {
				case *pb.ExecuteSqlRequest:
					if got := v.GetRequestOptions().GetPriority(); got != test.want {
						t.Errorf("priority mismatch: got = %v, want = %v", got, test.want)
					}
				case *pb.CommitRequest:
					if got := v.GetRequestOptions().GetPriority(); got != test.want {
						t.Errorf("priority mismatch: got = %v, want = %v", got, test.want)
					}
				}
			}
		})
	}
}

func TestParseDirectedReadOption(t *testing.T) {
	for _, tt := range []struct {
		desc   string
		option string
		want   *pb.DirectedReadOptions
	}{
		{
			desc:   "use directed read location option only",
			option: "us-central1",
			want: &pb.DirectedReadOptions{
				Replicas: &pb.DirectedReadOptions_IncludeReplicas_{
					IncludeReplicas: &pb.DirectedReadOptions_IncludeReplicas{
						ReplicaSelections: []*pb.DirectedReadOptions_ReplicaSelection{
							{
								Location: "us-central1",
							},
						},
						AutoFailoverDisabled: true,
					},
				},
			},
		},
		{
			desc:   "use directed read location and type option (READ_ONLY)",
			option: "us-central1:READ_ONLY",
			want: &pb.DirectedReadOptions{
				Replicas: &pb.DirectedReadOptions_IncludeReplicas_{
					IncludeReplicas: &pb.DirectedReadOptions_IncludeReplicas{
						ReplicaSelections: []*pb.DirectedReadOptions_ReplicaSelection{
							{
								Location: "us-central1",
								Type:     pb.DirectedReadOptions_ReplicaSelection_READ_ONLY,
							},
						},
						AutoFailoverDisabled: true,
					},
				},
			},
		},
		{
			desc:   "use directed read location and type option (READ_WRITE)",
			option: "us-central1:READ_WRITE",
			want: &pb.DirectedReadOptions{
				Replicas: &pb.DirectedReadOptions_IncludeReplicas_{
					IncludeReplicas: &pb.DirectedReadOptions_IncludeReplicas{
						ReplicaSelections: []*pb.DirectedReadOptions_ReplicaSelection{
							{
								Location: "us-central1",
								Type:     pb.DirectedReadOptions_ReplicaSelection_READ_WRITE,
							},
						},
						AutoFailoverDisabled: true,
					},
				},
			},
		},
		{
			desc:   "use invalid type option",
			option: "us-central1:READONLY",
			want:   nil,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got, _ := parseDirectedReadOption(tt.option)

			if !cmp.Equal(got, tt.want, protocmp.Transform()) {
				t.Errorf("Got = %v, but want = %v", got, tt.want)
			}
		})
	}
}

func setupTestServer(t *testing.T) *spannertest.Server {
	server, err := spannertest.NewServer("localhost:0")
	if err != nil {
		t.Fatalf("failed to run test server: %v", err)
	}
	ddl, err := spansql.ParseDDL("", "CREATE TABLE t1 (Id INT64) PRIMARY KEY (Id)")
	if err != nil {
		t.Fatalf("failed to parse DDL: %v", err)
	}
	if err := server.UpdateDDL(ddl); err != nil {
		t.Fatalf("failed to update DDL: %v", err)
	}
	return server
}

// requestRecorder is a recorder to retain gRPC requests for spannertest.Server.
type requestRecorder struct {
	requests []interface{}
}

func (r *requestRecorder) flush() {
	r.requests = nil
}

func recordRequestsInterceptors(recorder *requestRecorder) (grpc.UnaryClientInterceptor, grpc.StreamClientInterceptor) {
	unary := func(ctx context.Context, method string, req interface{}, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		recorder.requests = append(recorder.requests, req)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	stream := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		s, err := streamer(ctx, desc, cc, method, opts...)
		return &recordRequestsStream{recorder, s}, err
	}
	return unary, stream
}

type recordRequestsStream struct {
	recorder *requestRecorder
	grpc.ClientStream
}

func (s *recordRequestsStream) SendMsg(m interface{}) error {
	s.recorder.requests = append(s.recorder.requests, m)
	return s.ClientStream.SendMsg(m)
}
