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
	"strings"

	"github.com/xlab/treeprint"
	pb "google.golang.org/genproto/googleapis/spanner/v1"
)

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

func (n *Node) IsVisible() bool {
	operator := n.PlanNode.DisplayName
	if operator == "Function" || operator == "Reference" || operator == "Constant" {
		return false
	}

	return true
}

func (n *Node) String() string {
	operator := n.PlanNode.DisplayName
	metadata := getAllMetadataString(n)
	return operator + " " + metadata
}

func getMetadataString(node *Node, key string) (string, bool) {
	if node.PlanNode.Metadata == nil {
		return "", false
	}
	if v, ok := node.PlanNode.Metadata.Fields[key]; ok {
		return v.GetStringValue(), true
	} else {
		return "", false
	}
}

func getAllMetadataString(node *Node) string {
	if node.PlanNode.Metadata == nil {
		return ""
	}

	fields := make([]string, 0)
	for k, v := range node.PlanNode.Metadata.Fields {
		fields = append(fields, fmt.Sprintf("%s: %s", k, v.GetStringValue()))
	}
	return fmt.Sprintf(`(%s)`, strings.Join(fields, ", "))
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
