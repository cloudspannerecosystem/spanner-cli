package main

import (
	"github.com/xlab/treeprint"
	pb "google.golang.org/genproto/googleapis/spanner/v1"
)

type Node struct {
	PlanNode *pb.PlanNode
	Children []*Node
}

func BuildQueryPlanTree(plan *pb.QueryPlan, idx int32) *Node {
	if len(plan.PlanNodes) == 0 {
		// TODO
		return &Node{}
	}

	nodeMap := map[int32]*pb.PlanNode{}
	for _, node := range plan.PlanNodes {
		nodeMap[node.Index] = node
	}

	root := &Node{
		PlanNode: plan.PlanNodes[idx],
		Children: make([]*Node, 0),
	}
	if root.PlanNode.ChildLinks != nil {
		for _, childLink := range root.PlanNode.ChildLinks {
			idx := childLink.ChildIndex
			child := BuildQueryPlanTree(plan, idx)
			root.Children = append(root.Children, child)
		}
	}

	return root
}

func (n *Node) Render() string {
	tree := treeprint.New()
	renderTree(tree, n)
	return "\n" + tree.String()
}

func renderTree(tree treeprint.Tree, node *Node) {
	if len(node.Children) > 0 {
		branch := tree.AddBranch(node.PlanNode.DisplayName)
		for _, child := range node.Children {
			renderTree(branch, child)
		}
	} else {
		tree.AddNode(node.PlanNode.DisplayName)
	}
}
