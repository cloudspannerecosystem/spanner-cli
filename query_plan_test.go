package main

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

func mustNewStruct(m map[string]interface{}) *structpb.Struct {
	if s, err := structpb.NewStruct(m); err != nil {
		panic(err)
	} else {
		return s
	}
}

func TestRenderTreeUsingTestdataPlans(t *testing.T) {
	for _, test := range []struct {
		title string
		file  string
		want  []QueryPlanRow
	}{
		{
			// Original Query:
			// SELECT s.LastName FROM (SELECT s.LastName FROM Singers AS s WHERE s.FirstName LIKE 'A%' LIMIT 3) s WHERE s.LastName LIKE 'Rich%';
			title: "With Filter Operator",
			file:  "testdata/plans/filter.input.json",
			want: []QueryPlanRow{
				{
					ID:   0,
					Text: "Serialize Result",
				},
				{
					ID:         1,
					Text:       "+- Filter",
					Predicates: []string{"Condition: STARTS_WITH($LastName, 'Rich')"},
				},
				{
					ID:   2,
					Text: "   +- Global Limit",
				},
				{
					ID:         3,
					Text:       "      +- Distributed Union",
					Predicates: []string{"Split Range: STARTS_WITH($FirstName, 'A')"},
				},
				{
					ID:   4,
					Text: "         +- Local Limit",
				},
				{
					ID:   5,
					Text: "            +- Local Distributed Union",
				},
				{
					ID:         6,
					Text:       "               +- FilterScan",
					Predicates: []string{"Seek Condition: STARTS_WITH($FirstName, 'A')"},
				},
				{
					ID:   7,
					Text: "                  +- Index Scan (Index: SingersByFirstLastName)",
				},
			},
		},
		{
			/*
				Original Query:
				SELECT a.AlbumTitle, s.SongName
				FROM Albums AS a HASH JOIN Songs AS s
				ON a.SingerId = s.SingerId AND a.AlbumId = s.AlbumId;
			*/
			title: "Hash Join",
			file:  "testdata/plans/hash_join.input.json",
			want: []QueryPlanRow{
				{
					ID:   0,
					Text: "Distributed Union",
				},
				{
					ID:   1,
					Text: "+- Serialize Result",
				},
				{
					ID:         2,
					Text:       "   +- Hash Join (join_type: INNER)",
					Predicates: []string{"Condition: (($SingerId = $SingerId_1) AND ($AlbumId = $AlbumId_1))"},
				},
				{
					ID:   3,
					Text: "      +- [Build] Local Distributed Union",
				},
				{
					ID:   4,
					Text: "      |  +- Table Scan (Full scan: true, Table: Albums)",
				},
				{
					ID:   8,
					Text: "      +- [Probe] Local Distributed Union",
				},
				{
					ID:   9,
					Text: "         +- Index Scan (Full scan: true, Index: SongsBySingerAlbumSongNameDesc)",
				},
			},
		},
		{
			/*
				Original Query: https://cloud.google.com/spanner/docs/query-execution-operators?hl=en#array_subqueries
				SELECT a.AlbumId,
				ARRAY(SELECT ConcertDate
				      FROM Concerts
				      WHERE Concerts.SingerId = a.SingerId)
				FROM Albums AS a;
			*/
			title: "Array Subqueries",
			file:  "testdata/plans/array_subqueries.input.json",
			want: []QueryPlanRow{
				{
					ID:   0,
					Text: "Distributed Union",
				},
				{
					ID:   1,
					Text: "+- Local Distributed Union",
				},
				{
					ID:   2,
					Text: "   +- Serialize Result",
				},
				{
					ID:   3,
					Text: "      +- Index Scan (Full scan: true, Index: AlbumsByAlbumTitle)",
				},
				{
					ID:   7,
					Text: "      +- [Scalar] Array Subquery",
				},
				{
					ID:         8,
					Text:       "         +- Distributed Union",
					Predicates: []string{"Split Range: ($SingerId_1 = $SingerId)"},
				},
				{
					ID:   9,
					Text: "            +- Local Distributed Union",
				},
				{
					ID:         10,
					Text:       "               +- FilterScan",
					Predicates: []string{"Seek Condition: ($SingerId_1 = $SingerId)"},
				},
				{
					ID:   11,
					Text: "                  +- Index Scan (Index: ConcertsBySingerId)",
				},
			},
		},
		{
			/*
				Original Query: https://cloud.google.com/spanner/docs/query-execution-operators?hl=en#scalar_subqueries
				SELECT FirstName,
				IF(FirstName='Alice',
				   (SELECT COUNT(*)
				    FROM Songs
				    WHERE Duration > 300),
				   0)
				FROM Singers;
			*/
			title: "Scalar Subqueries",
			file:  "testdata/plans/scalar_subqueries.input.json",
			want: []QueryPlanRow{
				{
					Text: "Distributed Union",
				},
				{
					ID:   1,
					Text: "+- Local Distributed Union",
				},
				{
					ID:   2,
					Text: "   +- Serialize Result",
				},
				{
					ID:   3,
					Text: "      +- Index Scan (Full scan: true, Index: SingersByFirstLastName)",
				},
				{
					ID:   10,
					Text: "      +- [Scalar] Scalar Subquery",
				},
				{
					ID:   11,
					Text: "         +- Global Stream Aggregate (scalar_aggregate: true)",
				},
				{
					ID:   12,
					Text: "            +- Distributed Union",
				},
				{
					ID:   13,
					Text: "               +- Local Stream Aggregate (scalar_aggregate: true)",
				},
				{
					ID:   14,
					Text: "                  +- Local Distributed Union",
				},
				{
					ID:   15,
					Text: "                     +- FilterScan",
					Predicates: []string{
						"Residual Condition: ($Duration > 300)",
					},
				},
				{
					ID:   16,
					Text: "                        +- Table Scan (Full scan: true, Table: Songs)",
				},
			},
		},
		{
			/*
				Original Query:
				SELECT si.*,
				  ARRAY(SELECT AS STRUCT a.*,
				        ARRAY(SELECT AS STRUCT so.*
				              FROM Songs so
				              WHERE a.SingerId = so.SingerId AND a.AlbumId = so.AlbumId)
				        FROM Albums a
				        WHERE a.SingerId = si.SingerId)
				FROM Singers si;
			*/
			title: "Array Subquery with Compute Struct",
			file:  "testdata/plans/array_subqueries_with_compute_struct.input.json",
			want: []QueryPlanRow{
				{
					Text: "Distributed Union",
				},
				{
					ID:   1,
					Text: "+- Local Distributed Union",
				},
				{
					ID:   2,
					Text: "   +- Serialize Result",
				},
				{
					ID:   3,
					Text: "      +- Table Scan (Full scan: true, Table: Singers)",
				},
				{
					ID:   14,
					Text: "      +- [Scalar] Array Subquery",
				},
				{
					ID:   15,
					Text: "         +- Local Distributed Union",
				},
				{
					ID:   16,
					Text: "            +- Compute Struct",
				},
				{
					ID:   17,
					Text: "               +- FilterScan",
					Predicates: []string{
						"Seek Condition: ($SingerId_1 = $SingerId)",
					},
				},
				{
					ID:   18,
					Text: "               |  +- Table Scan (Table: Albums)",
				},
				{
					ID:   31,
					Text: "               +- [Scalar] Array Subquery",
				},
				{
					ID:   32,
					Text: "                  +- Local Distributed Union",
				},
				{
					ID:   33,
					Text: "                     +- Compute Struct",
				},
				{
					ID:   34,
					Text: "                        +- FilterScan",
					Predicates: []string{
						"Seek Condition: (($SingerId_2 = $SingerId_1) AND ($AlbumId_1 = $AlbumId))",
					},
				},
				{
					ID:   35,
					Text: "                           +- Table Scan (Table: Songs)",
				},
			},
		},
		{
			/*
				Original Query:
				SELECT so.* FROM Songs so
				WHERE IF(so.SongGenre = "ROCKS", TRUE, EXISTS(SELECT * FROM Concerts c WHERE c.SingerId = so.SingerId))
			*/
			title: "Scalar Subquery with FilterScan",
			file:  "testdata/plans/scalar_subquery_with_filter_scan.input.json",
			want: []QueryPlanRow{
				{
					Text: "Distributed Union",
				},
				{
					ID:   1,
					Text: "+- Local Distributed Union",
				},
				{
					ID:   2,
					Text: "   +- Serialize Result",
				},
				{
					ID:   3,
					Text: "      +- FilterScan",
					Predicates: []string{
						"Residual Condition: IF(($SongGenre = 'ROCKS'), true, $sv_1)",
					},
				},
				{
					ID:   4,
					Text: "         +- Table Scan (Full scan: true, Table: Songs)",
				},
				{
					ID:   16,
					Text: "         +- [Scalar] Scalar Subquery",
				},
				{
					ID:   17,
					Text: "            +- Global Stream Aggregate (scalar_aggregate: true)",
				},
				{
					ID:   18,
					Text: "               +- Distributed Union",
					Predicates: []string{
						"Split Range: ($SingerId_1 = $SingerId)",
					},
				},
				{
					ID:   19,
					Text: "                  +- Local Stream Aggregate (scalar_aggregate: true)",
				},
				{
					ID:   20,
					Text: "                     +- Local Distributed Union",
				},
				{
					ID:   21,
					Text: "                        +- FilterScan",
					Predicates: []string{
						"Seek Condition: ($SingerId_1 = $SingerId)",
					},
				},
				{
					ID:   22,
					Text: "                           +- Index Scan (Index: ConcertsBySingerId)",
				},
			},
		},
	} {
		t.Run(test.title, func(t *testing.T) {
			b, err := os.ReadFile(test.file)
			if err != nil {
				t.Fatal(err)
			}
			var plan pb.QueryPlan
			err = protojson.Unmarshal(b, &plan)
			if err != nil {
				t.Fatal(err)
			}
			tree := BuildQueryPlanTree(&plan, 0)
			got, err := tree.RenderTreeWithStats(plan.GetPlanNodes())
			if err != nil {
				t.Errorf("error should be nil, but got = %v", err)
			}
			if !cmp.Equal(test.want, got) {
				t.Errorf("node.RenderTreeWithStats() differ: %s", cmp.Diff(test.want, got))
			}
		})
	}
}

func TestRenderTreeWithStats(t *testing.T) {
	for _, test := range []struct {
		title string
		plan  *spanner.QueryPlan
		want  []QueryPlanRow
	}{
		{
			title: "Simple Query",
			plan: &spanner.QueryPlan{
				PlanNodes: []*spanner.PlanNode{
					{
						Index: 0,
						ChildLinks: []*spanner.PlanNode_ChildLink{
							{ChildIndex: 1},
						},
						DisplayName: "Distributed Union",
						Kind:        spanner.PlanNode_RELATIONAL,
						ExecutionStats: mustNewStruct(map[string]interface{}{
							"latency":           map[string]interface{}{"total": "1", "unit": "msec"},
							"rows":              map[string]interface{}{"total": "9"},
							"execution_summary": map[string]interface{}{"num_executions": "1"},
						}),
					},
					{
						Index: 1,
						ChildLinks: []*spanner.PlanNode_ChildLink{
							{ChildIndex: 2},
						},
						DisplayName: "Distributed Union",
						Kind:        spanner.PlanNode_RELATIONAL,
						Metadata:    mustNewStruct(map[string]interface{}{"call_type": "Local"}),
						ExecutionStats: mustNewStruct(map[string]interface{}{
							"latency":           map[string]interface{}{"total": "1", "unit": "msec"},
							"rows":              map[string]interface{}{"total": "9"},
							"execution_summary": map[string]interface{}{"num_executions": "1"},
						}),
					},
					{
						Index: 2,
						ChildLinks: []*spanner.PlanNode_ChildLink{
							{ChildIndex: 3},
						},
						DisplayName: "Serialize Result",
						Kind:        spanner.PlanNode_RELATIONAL,
						ExecutionStats: mustNewStruct(map[string]interface{}{
							"latency":           map[string]interface{}{"total": "1", "unit": "msec"},
							"rows":              map[string]interface{}{"total": "9"},
							"execution_summary": map[string]interface{}{"num_executions": "1"},
						}),
					},
					{
						Index:       3,
						DisplayName: "Scan",
						Kind:        spanner.PlanNode_RELATIONAL,
						Metadata:    mustNewStruct(map[string]interface{}{"scan_type": "IndexScan", "scan_target": "SongsBySingerAlbumSongNameDesc", "Full scan": "true"}),
						ExecutionStats: mustNewStruct(map[string]interface{}{
							"latency":           map[string]interface{}{"total": "1", "unit": "msec"},
							"rows":              map[string]interface{}{"total": "9"},
							"execution_summary": map[string]interface{}{"num_executions": "1"},
						}),
					},
				},
			},
			want: []QueryPlanRow{
				{
					ID:           0,
					Text:         "Distributed Union",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
				{
					ID:           1,
					Text:         "+- Local Distributed Union",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
				{
					ID:           2,
					Text:         "   +- Serialize Result",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
				{
					ID:           3,
					Text:         "      +- Index Scan (Full scan: true, Index: SongsBySingerAlbumSongNameDesc)",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
			},
		},
	} {
		t.Run(test.title, func(t *testing.T) {
			tree := BuildQueryPlanTree(test.plan, 0)
			got, err := tree.RenderTreeWithStats(test.plan.GetPlanNodes())
			if err != nil {
				t.Errorf("error should be nil, but got = %v", err)
			}
			if !cmp.Equal(test.want, got) {
				t.Errorf("node.RenderTreeWithStats() differ: %s", cmp.Diff(test.want, got))
			}
		})
	}
}

func TestNodeString(t *testing.T) {
	for _, test := range []struct {
		title string
		node  *Node
		want  string
	}{
		{
			"Distributed Union with call_type=Local",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Distributed Union",
				Metadata: mustNewStruct(map[string]interface{}{
					"call_type":             "Local",
					"subquery_cluster_node": "4",
				}),
			}}, "Local Distributed Union",
		},
		{
			"Scan with scan_type=IndexScan and Full scan=true",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: mustNewStruct(map[string]interface{}{
					"scan_type":   "IndexScan",
					"scan_target": "SongsBySongName",
					"Full scan":   "true",
				}),
			}}, "Index Scan (Full scan: true, Index: SongsBySongName)",
		},
		{
			"Scan with scan_type=TableScan",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: mustNewStruct(map[string]interface{}{
					"scan_type":   "TableScan",
					"scan_target": "Songs",
				}),
			}}, "Table Scan (Table: Songs)",
		},
		{
			"Scan with scan_type=BatchScan",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: mustNewStruct(map[string]interface{}{
					"scan_type":   "BatchScan",
					"scan_target": "$v2",
				}),
			}}, "Batch Scan (Batch: $v2)",
		},
		{
			"Sort Limit with call_type=Local",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Sort Limit",
				Metadata: mustNewStruct(map[string]interface{}{
					"call_type": "Local",
				}),
			}}, "Local Sort Limit",
		},
		{
			"Sort Limit with call_type=Global",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Sort Limit",
				Metadata: mustNewStruct(map[string]interface{}{
					"call_type": "Global",
				}),
			}}, "Global Sort Limit",
		},
		{
			"Aggregate with iterator_type=Stream",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Aggregate",
				Metadata: mustNewStruct(map[string]interface{}{
					"iterator_type": "Stream",
				}),
			}}, "Stream Aggregate",
		},
	} {
		if got := test.node.String(); got != test.want {
			t.Errorf("%s: node.String() = %q but want %q", test.title, got, test.want)
		}
	}
}

func TestGetMaxRelationalNodeID(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		input *pb.QueryPlan
		want  int32
	}{
		{
			desc: "pre-sorted order",
			input: &pb.QueryPlan{
				PlanNodes: []*pb.PlanNode{
					{Index: 0, DisplayName: "Scalar Subquery", Kind: pb.PlanNode_SCALAR},
					{Index: 1, DisplayName: "Index Scan", Kind: pb.PlanNode_RELATIONAL},
					{Index: 2, DisplayName: "Index Scan", Kind: pb.PlanNode_RELATIONAL},
					{Index: 3, DisplayName: "Index Scan", Kind: pb.PlanNode_RELATIONAL},
					{Index: 4, DisplayName: "Constant", Kind: pb.PlanNode_SCALAR}, // This is not visible
				},
			},
			want: 3,
		},
	} {
		if got := getMaxRelationalNodeID(tt.input); got != tt.want {
			t.Errorf("getMaxRelationalNodeID(%s) = %d, but want = %d", tt.input, got, tt.want)
		}
	}
}
