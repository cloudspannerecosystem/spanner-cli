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
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/xlab/treeprint"
	"google.golang.org/protobuf/types/known/structpb"
)

func init() {
	// Use only ascii characters
	treeprint.EdgeTypeLink = "|"
	treeprint.EdgeTypeMid = "+-"
	treeprint.EdgeTypeEnd = "+-"

	treeprint.IndentSize = 2
}

type Link struct {
	Dest *Node
	Type string
}

type Node struct {
	PlanNode *pb.PlanNode
	Children []*Link
}

type QueryPlanNodeWithStats struct {
	ID             int32            `json:"id"`
	ExecutionStats *structpb.Struct `json:"execution_stats"`
	DisplayName    string           `json:"display_name"`
	LinkType       string           `json:"link_type"`
}

type executionStatsValue struct {
	Unit  string `json:"unit"`
	Total string `json:"total"`
}

func (v executionStatsValue) String() string {
	if v.Unit == "" {
		return v.Total
	} else {
		return fmt.Sprintf("%s %s", v.Total, v.Unit)
	}
}

// queryPlanNodeWithStatsTyped is proto-free typed representation of QueryPlanNodeWithStats
type queryPlanNodeWithStatsTyped struct {
	ID             int32 `json:"id"`
	ExecutionStats struct {
		Rows             executionStatsValue `json:"rows"`
		Latency          executionStatsValue `json:"latency"`
		ExecutionSummary struct {
			NumExecutions string `json:"num_executions"`
		} `json:"execution_summary"`
	} `json:"execution_stats"`
	DisplayName string `json:"display_name"`
	LinkType    string `json:"link_type"`
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

type QueryPlanRow struct {
	ID           int32
	Text         string
	RowsTotal    string
	Execution    string
	LatencyTotal string
	Predicates   []string
}

func isPredicate(planNodes []*pb.PlanNode, childLink *pb.PlanNode_ChildLink) bool {
	// Known predicates are Condition(Filter/Hash Join) or Seek Condition/Residual Condition(FilterScan) or Split Range(Distributed Union).
	// Agg is a Function but not a predicate.
	child := planNodes[childLink.ChildIndex]
	if child.DisplayName != "Function" {
		return false
	}
	if strings.HasSuffix(childLink.GetType(), "Condition") || childLink.GetType() == "Split Range" {
		return true
	}
	return false
}

func (n *Node) RenderTreeWithStats(planNodes []*pb.PlanNode) ([]QueryPlanRow, error) {
	tree := treeprint.New()
	renderTreeWithStats(tree, "", n)
	var result []QueryPlanRow
	for _, line := range strings.Split(tree.String(), "\n") {
		if line == "" {
			continue
		}

		split := strings.SplitN(line, "\t", 2)
		// Handle the case of the root node of treeprint
		if len(split) != 2 {
			return nil, fmt.Errorf("unexpected split error, tree line = %q", line)
		}
		branchText, protojsonText := split[0], split[1]

		var planNode queryPlanNodeWithStatsTyped
		if err := json.Unmarshal([]byte(protojsonText), &planNode); err != nil {
			return nil, fmt.Errorf("unexpected JSON unmarshal error, tree line = %q", line)
		}

		var text string
		if planNode.LinkType != "" {
			text = fmt.Sprintf("[%s] %s", planNode.LinkType, planNode.DisplayName)
		} else {
			text = planNode.DisplayName
		}

		var predicates []string
		for _, cl := range planNodes[planNode.ID].GetChildLinks() {
			if !isPredicate(planNodes, cl) {
				continue
			}
			predicates = append(predicates, fmt.Sprintf("%s: %s", cl.GetType(), planNodes[cl.ChildIndex].GetShortRepresentation().GetDescription()))
		}

		result = append(result, QueryPlanRow{
			ID:           planNode.ID,
			Predicates:   predicates,
			Text:         branchText + text,
			RowsTotal:    planNode.ExecutionStats.Rows.Total,
			Execution:    planNode.ExecutionStats.ExecutionSummary.NumExecutions,
			LatencyTotal: planNode.ExecutionStats.Latency.String(),
		})
	}
	return result, nil
}

func (n *Node) IsRoot() bool {
	return n.PlanNode.Index == 0
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

func renderTreeWithStats(tree treeprint.Tree, linkType string, node *Node) {
	// Scalar operator is rendered if and only if it is linked as Scalar type(Scalar/Array Subquery)
	if node.PlanNode.GetKind() == pb.PlanNode_SCALAR && linkType != "Scalar" {
		return
	}

	b, _ := json.Marshal(
		QueryPlanNodeWithStats{
			ID:             node.PlanNode.Index,
			ExecutionStats: node.PlanNode.GetExecutionStats(),
			DisplayName:    node.String(),
			LinkType:       linkType,
		},
	)
	// Prefixed by tab to ease to split
	str := "\t" + string(b)

	if len(node.Children) > 0 {
		var branch treeprint.Tree
		if node.IsRoot() {
			tree.SetValue(str)
			branch = tree
		} else {
			branch = tree.AddBranch(str)
		}
		for _, child := range node.Children {
			renderTreeWithStats(branch, child.Type, child.Dest)
		}
	} else {
		if node.IsRoot() {
			tree.SetValue(str)
		} else {
			tree.AddNode(str)
		}
	}
}

func getMaxRelationalNodeID(plan *pb.QueryPlan) int32 {
	var maxRelationalNodeID int32
	// We assume that plan_nodes[] is pre-sorted in ascending order.
	// See QueryPlan.plan_nodes[] in the document.
	// https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1?hl=en#google.spanner.v1.QueryPlan.FIELDS.repeated.google.spanner.v1.PlanNode.google.spanner.v1.QueryPlan.plan_nodes
	for _, planNode := range plan.GetPlanNodes() {
		if planNode.GetKind() == pb.PlanNode_RELATIONAL {
			maxRelationalNodeID = planNode.Index
		}
	}
	return maxRelationalNodeID
}
