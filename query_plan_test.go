package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func protojsonAsStruct(t *testing.T, j string) *structpb.Struct {
	t.Helper()
	var result structpb.Struct
	if err := protojson.Unmarshal([]byte(j), &result); err != nil {
		t.Fatal("protojsonAsStruct fails, invalid test case", err)
	}
	return &result
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
						ExecutionStats: protojsonAsStruct(t, `
{
  "latency": {"total": "1", "unit": "msec"},
  "rows": {"total": "9"},
  "execution_summary": {"num_executions": "1"}
}`),
					},
					{
						Index: 1,
						ChildLinks: []*spanner.PlanNode_ChildLink{
							{ChildIndex: 2},
						},
						DisplayName: "Distributed Union",
						Kind:        spanner.PlanNode_RELATIONAL,
						Metadata:    protojsonAsStruct(t, `{"call_type": "Local"}`),
						ExecutionStats: protojsonAsStruct(t, `
{
  "latency": {"total": "1", "unit": "msec"},
  "rows": {"total": "9"},
  "execution_summary": {"num_executions": "1"}
}`),
					},
					{
						Index: 2,
						ChildLinks: []*spanner.PlanNode_ChildLink{
							{ChildIndex: 3},
						},
						DisplayName: "Serialize Result",
						Kind:        spanner.PlanNode_RELATIONAL,
						ExecutionStats: protojsonAsStruct(t, `
{
  "latency": {"total": "1", "unit": "msec"},
  "rows": {"total": "9"},
  "execution_summary": {"num_executions": "1"}
}`),
					},
					{
						Index:       3,
						DisplayName: "Scan",
						Kind:        spanner.PlanNode_RELATIONAL,
						Metadata:    protojsonAsStruct(t, `{"scan_type": "IndexScan", "scan_target": "SongsBySingerAlbumSongNameDesc", "Full scan": "true"}`),
						ExecutionStats: protojsonAsStruct(t, `
{
  "latency": {"total": "1", "unit": "msec"},
  "rows": {"total": "9"},
  "execution_summary": {"num_executions": "1"}
}`),
					},
				},
			},
			want: []QueryPlanRow{
				{Text: ".", TextOnly: true},
				{
					ID:           0,
					Text:         "+- Distributed Union",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
				{
					ID:           1,
					Text:         "    +- Local Distributed Union",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
				{
					ID:           2,
					Text:         "        +- Serialize Result",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
				{
					ID:           3,
					Text:         "            +- Index Scan (Full scan: true, Index: SongsBySingerAlbumSongNameDesc)",
					RowsTotal:    "9",
					Execution:    "1",
					LatencyTotal: "1 msec",
				},
			}},
	} {
		tree := BuildQueryPlanTree(test.plan, 0)
		if got := tree.RenderTreeWithStats(test.plan.GetPlanNodes()); !cmp.Equal(test.want, got) {
			t.Errorf("%s: node.RenderTreeWithStats() differ: %s", test.title, cmp.Diff(test.want, got))
		}
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
				Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"call_type":             {Kind: &structpb.Value_StringValue{StringValue: "Local"}},
						"subquery_cluster_node": {Kind: &structpb.Value_StringValue{StringValue: "4"}},
					},
				},
			}}, "Local Distributed Union",
		},
		{"Scan with scan_type=IndexScan and Full scan=true",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"scan_type":   {Kind: &structpb.Value_StringValue{StringValue: "IndexScan"}},
						"scan_target": {Kind: &structpb.Value_StringValue{StringValue: "SongsBySongName"}},
						"Full scan":   {Kind: &structpb.Value_StringValue{StringValue: "true"}},
					},
				},
			}}, "Index Scan (Full scan: true, Index: SongsBySongName)"},
		{"Scan with scan_type=TableScan",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"scan_type":   {Kind: &structpb.Value_StringValue{StringValue: "TableScan"}},
						"scan_target": {Kind: &structpb.Value_StringValue{StringValue: "Songs"}},
					},
				},
			}}, "Table Scan (Table: Songs)"},
		{"Scan with scan_type=BatchScan",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Scan",
				Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"scan_type":   {Kind: &structpb.Value_StringValue{StringValue: "BatchScan"}},
						"scan_target": {Kind: &structpb.Value_StringValue{StringValue: "$v2"}},
					},
				},
			}}, "Batch Scan (Batch: $v2)"},
		{"Sort Limit with call_type=Local",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Sort Limit",
				Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"call_type": {Kind: &structpb.Value_StringValue{StringValue: "Local"}},
					},
				},
			}}, "Local Sort Limit"},
		{"Sort Limit with call_type=Global",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Sort Limit",
				Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"call_type": {Kind: &structpb.Value_StringValue{StringValue: "Global"}},
					},
				},
			}}, "Global Sort Limit"},
		{"Aggregate with iterator_type=Stream",
			&Node{PlanNode: &spanner.PlanNode{
				DisplayName: "Aggregate",
				Metadata: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"iterator_type": {Kind: &structpb.Value_StringValue{StringValue: "Stream"}},
					},
				},
			}}, "Stream Aggregate"},
	} {
		if got := test.node.String(); got != test.want {
			t.Errorf("%s: node.String() = %q but want %q", test.title, got, test.want)
		}
	}
}
