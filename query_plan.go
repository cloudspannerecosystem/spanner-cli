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
	"fmt"
	"sort"
	"strings"

	"github.com/xlab/treeprint"
	pb "google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func init() {
	// Use only ascii characters
	treeprint.EdgeTypeLink = "|"
	treeprint.EdgeTypeMid = "+-"
	treeprint.EdgeTypeEnd = "+-"
}

type Link struct {
	Dest *Node
	Type string
}

type Node struct {
	PlanNode *pb.PlanNode
	Children []*Link
}

func BuildQueryPlanTree(plan *pb.QueryPlan, idx int32) *Node {
	if len(plan.PlanNodes) == 0 {
		return &Node{}
	}

	nodeMap := map[int32]*pb.PlanNode{}
	for _, node := range plan.PlanNodes {
		nodeMap[node.Index] = node
	}

	root := &Node{
		PlanNode: plan.PlanNodes[idx],
		Children: make([]*Link, 0),
	}
	if root.PlanNode.ChildLinks != nil {
		for i, childLink := range root.PlanNode.ChildLinks {
			idx := childLink.ChildIndex
			child := BuildQueryPlanTree(plan, idx)
			childType := childLink.Type

			// Fill missing Input type into the first child of [Distributed] (Cross|Outer) Apply
			if childType == "" && strings.HasSuffix(root.PlanNode.DisplayName, "Apply") && i == 0 {
				childType = "Input"
			}
			root.Children = append(root.Children, &Link{Type: childType, Dest: child})
		}
	}

	return root
}

func (n *Node) Render() string {
	tree := treeprint.New()
	renderTree(tree, "", n)
	return "\n" + tree.String()
}

type RenderedTreeWithStats struct {
	Text         string
	RowsTotal    string
	Execution    string
	LatencyTotal string
}

func (n *Node) RenderTreeWithStats() []RenderedTreeWithStats {
	tree := treeprint.New()
	renderTreeWithStats(tree, "", n)
	var result []RenderedTreeWithStats
	for _, line := range strings.Split(tree.String(), "\n") {
		if line == "" {
			continue
		}
		matched := strings.Split(line, "\t")
		if len(matched) == 1 {
			result = append(result, RenderedTreeWithStats{Text: matched[0]})
			continue
		}
		var value structpb.Value
		err := protojson.Unmarshal([]byte(matched[1]), &value)
		if err != nil {
			result = append(result, RenderedTreeWithStats{Text: matched[0]})
			continue
		}
		result = append(result, RenderedTreeWithStats{
			Text:         matched[0],
			RowsTotal:    getStringValueByPath(value.GetStructValue(), "rows", "total"),
			Execution:    getStringValueByPath(value.GetStructValue(), "execution_summary", "num_executions"),
			LatencyTotal: getStringValueByPath(value.GetStructValue(), "latency", "total"),
		})
	}
	return result
}

func (n *Node) IsVisible() bool {
	operator := n.PlanNode.DisplayName
	if operator == "Function" || operator == "Reference" || operator == "Constant" {
		return false
	}

	return true
}

func (n *Node) String() string {
	metadataFields := n.PlanNode.GetMetadata().GetFields()

	var operator string
	{
		var components []string
		for _, s := range []string{
			metadataFields["call_type"].GetStringValue(),
			metadataFields["iterator_type"].GetStringValue(),
			strings.TrimSuffix(metadataFields["scan_type"].GetStringValue(), "Scan"),
			n.PlanNode.GetDisplayName(),
		} {
			if s != "" {
				components = append(components, s)
			}
		}
		operator = strings.Join(components, " ")
	}

	var metadata string
	{
		fields := make([]string, 0)
		for k, v := range metadataFields {
			switch k {
			case "call_type", "iterator_type": // Skip because it is displayed in node title
				continue
			case "scan_target": // Skip because it is combined with scan_type
				continue
			case "subquery_cluster_node": // Skip because it is useless without displaying node id
				continue
			case "scan_type":
				fields = append(fields, fmt.Sprintf("%s: %s",
					strings.TrimSuffix(v.GetStringValue(), "Scan"),
					metadataFields["scan_target"].GetStringValue()))
			default:
				fields = append(fields, fmt.Sprintf("%s: %s", k, v.GetStringValue()))
			}
		}

		sort.Strings(fields)

		if len(fields) != 0 {
			metadata = fmt.Sprintf(`(%s)`, strings.Join(fields, ", "))
		}
	}

	if metadata == "" {
		return operator
	}
	return operator + " " + metadata
}

func renderTree(tree treeprint.Tree, linkType string, node *Node) {
	if !node.IsVisible() {
		return
	}

	str := node.String()

	if len(node.Children) > 0 {
		var branch treeprint.Tree
		if linkType != "" {
			branch = tree.AddMetaBranch(linkType, str)
		} else {
			branch = tree.AddBranch(str)
		}
		for _, child := range node.Children {
			renderTree(branch, child.Type, child.Dest)
		}
	} else {
		if linkType != "" {
			tree.AddMetaNode(linkType, str)
		} else {
			tree.AddNode(str)
		}
	}
}

func getStringValueByPath(s *structpb.Struct, first string, path ...string) string {
	current := s.GetFields()[first]
	for _, p := range path {
		current = current.GetStructValue().GetFields()[p]
	}
	return current.GetStringValue()
}

func renderTreeWithStats(tree treeprint.Tree, linkType string, node *Node) {
	if !node.IsVisible() {
		return
	}

	b, _ := protojson.Marshal(
			&structpb.Value{Kind: &structpb.Value_StructValue{node.PlanNode.GetExecutionStats()}},
	)
	str := node.String() + "\t" + string(b)

	if len(node.Children) > 0 {
		var branch treeprint.Tree
		if linkType != "" {
			branch = tree.AddMetaBranch(linkType, str)
		} else {
			branch = tree.AddBranch(str)
		}
		for _, child := range node.Children {
			renderTreeWithStats(branch, child.Type, child.Dest)
		}
	} else {
		if linkType != "" {
			tree.AddMetaNode(linkType, str)
		} else {
			tree.AddNode(str)
		}
	}
}
