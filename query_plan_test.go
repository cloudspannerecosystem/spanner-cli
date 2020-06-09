package main

import (
	"testing"

	"google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNodeString(t *testing.T) {
	for _, test := range []struct {
		title               string
		node               *Node
		want string
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
		{ "Scan with scan_type=TableScan",
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
