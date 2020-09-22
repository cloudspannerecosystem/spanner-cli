package main

import (
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	pb "google.golang.org/genproto/googleapis/spanner/v1"
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
			title: "Simple Query",
			file: "testdata/plans/filter.input.json",
			want: []QueryPlanRow{
				{
					ID:           0,
					Text:         "Serialize Result",
				},
				{
					ID:           1,
					Text:         "+- Filter",
					Predicates: []string{"Condition: STARTS_WITH($LastName, 'Rich')"},
				},
				{
					ID:           2,
					Text:         "   +- Global Limit",
				},
				{
					ID:           3,
					Text:         "      +- Distributed Union",
					Predicates: []string{"Split Range: STARTS_WITH($FirstName, 'A')"},
				},
				{
					ID:           4,
					Text:         "         +- Local Limit",
				},
				{
					ID:           5,
					Text:         "            +- Local Distributed Union",
				},
				{
					ID:           6,
					Text:         "               +- FilterScan",
					Predicates: []string{"Seek Condition: STARTS_WITH($FirstName, 'A')"},
				},
				{
					ID:           7,
					Text:         "                  +- Index Scan (Index: SingersByFirstLastName)",
				},
			}},
	} {
		t.Run(test.title, func(t *testing.T) {
			b, err := ioutil.ReadFile(test.file)
			if err != nil {
				t.Error(err)
			}
			var plan pb.QueryPlan
			err = protojson.Unmarshal(b, &plan)
			if err != nil {
				t.Error(err)
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
			}},
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
		{"Distributed Union with call_type=Local",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Distributed Union",
				Metadata: mustNewStruct(map[string]interface{}{
					"call_type":             "Local",
					"subquery_cluster_node": "4",
				}),
			}}, "Local Distributed Union",
		},
		{"Scan with scan_type=IndexScan and Full scan=true",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: mustNewStruct(map[string]interface{}{
					"scan_type":   "IndexScan",
					"scan_target": "SongsBySongName",
					"Full scan":   "true",
				}),
			}}, "Index Scan (Full scan: true, Index: SongsBySongName)"},
		{"Scan with scan_type=TableScan",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: mustNewStruct(map[string]interface{}{
					"scan_type":   "TableScan",
					"scan_target": "Songs",
				}),
			}}, "Table Scan (Table: Songs)"},
		{"Scan with scan_type=BatchScan",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: mustNewStruct(map[string]interface{}{
					"scan_type":   "BatchScan",
					"scan_target": "$v2",
				}),
			}}, "Batch Scan (Batch: $v2)"},
		{"Sort Limit with call_type=Local",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Sort Limit",
				Metadata: mustNewStruct(map[string]interface{}{
					"call_type": "Local",
				}),
			}}, "Local Sort Limit"},
		{"Sort Limit with call_type=Global",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Sort Limit",
				Metadata: mustNewStruct(map[string]interface{}{
					"call_type": "Global",
				}),
			}}, "Global Sort Limit"},
		{"Aggregate with iterator_type=Stream",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Aggregate",
				Metadata: mustNewStruct(map[string]interface{}{
					"iterator_type": "Stream",
				}),
			}}, "Stream Aggregate"},
	} {
		if got := test.node.String(); got != test.want {
			t.Errorf("%s: node.String() = %q but want %q", test.title, got, test.want)
		}
	}
}

func TestGetMaxVisibleNodeID(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		input *pb.QueryPlan
		want  int32
	}{
		{
			desc: "pre-sorted order",
			input: &pb.QueryPlan{
				PlanNodes: []*pb.PlanNode{
					{Index: 1, DisplayName: "Index Scan"},
					{Index: 2, DisplayName: "Index Scan"},
					{Index: 3, DisplayName: "Index Scan"},
					{Index: 4, DisplayName: "Constant"}, // This is not visible
				},
			},
			want: 3,
		},
	} {
		if got := getMaxVisibleNodeID(tt.input); got != tt.want {
			t.Errorf("getMaxVisibleNodeID(%s) = %d, but want = %d", tt.input, got, tt.want)
		}
	}
}
