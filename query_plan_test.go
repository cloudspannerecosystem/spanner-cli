package main

import (
	"testing"

	"google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGetNodeTitleAndGetAllMetadataString(t *testing.T) {
	for _, test := range []struct {
		node               *Node
		wantNodeTitle      string
		wantMetadataString string
	}{
		{&Node{PlanNode: &spanner.PlanNode{
			DisplayName: "Distributed Union",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"call_type":             {Kind: &structpb.Value_StringValue{StringValue: "Local"}},
					"subquery_cluster_node": {Kind: &structpb.Value_StringValue{StringValue: "4"}},
				},
			},
		}}, "Local Distributed Union", "",
		},
		{&Node{PlanNode: &spanner.PlanNode{
			DisplayName: "Scan",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"scan_type":   {Kind: &structpb.Value_StringValue{StringValue: "IndexScan"}},
					"scan_target": {Kind: &structpb.Value_StringValue{StringValue: "SongsBySongName"}},
					"Full scan":   {Kind: &structpb.Value_StringValue{StringValue: "true"}},
				},
			},
		}}, "Index Scan", "(Full scan: true, Index: SongsBySongName)"},
		{&Node{PlanNode: &spanner.PlanNode{
			DisplayName: "Scan",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"scan_type":   {Kind: &structpb.Value_StringValue{StringValue: "TableScan"}},
					"scan_target": {Kind: &structpb.Value_StringValue{StringValue: "Songs"}},
				},
			},
		}}, "Table Scan", "(Table: Songs)"},
		{&Node{PlanNode: &spanner.PlanNode{
			DisplayName: "Scan",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"scan_type":   {Kind: &structpb.Value_StringValue{StringValue: "BatchScan"}},
					"scan_target": {Kind: &structpb.Value_StringValue{StringValue: "$v2"}},
				},
			},
		}}, "Batch Scan", "(Batch: $v2)"},
		{&Node{PlanNode: &spanner.PlanNode{
			DisplayName: "Sort Limit",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"call_type": {Kind: &structpb.Value_StringValue{StringValue: "Local"}},
				},
			},
		}}, "Local Sort Limit", ""},
		{&Node{PlanNode: &spanner.PlanNode{
			DisplayName: "Sort Limit",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"call_type": {Kind: &structpb.Value_StringValue{StringValue: "Global"}},
				},
			},
		}}, "Global Sort Limit", ""},
		{&Node{PlanNode: &spanner.PlanNode{
			DisplayName: "Aggregate",
			Metadata: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"iterator_type": {Kind: &structpb.Value_StringValue{StringValue: "Stream"}},
				},
			},
		}}, "Stream Aggregate", ""},
	} {
		if got := getNodeTitle(test.node); got != test.wantNodeTitle {
			t.Errorf("getNodeTitle(%#v) = %q but want %q", test.node, got, test.wantNodeTitle)
		}
		if got := getAllMetadataString(test.node); got != test.wantMetadataString {
			t.Errorf("getAllMetadataString(%#v) = %q but want %q", test.node, got, test.wantMetadataString)
		}
	}
}
