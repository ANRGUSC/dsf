package dagconstraints

import (
	"context"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = "DAGConstraints"

// DAGConstraints is a plugin that filters and scores nodes based on DAG constraints.
type DAGConstraints struct {
	handle framework.Handle
}

var _ framework.FilterPlugin = &DAGConstraints{}
var _ framework.ScorePlugin = &DAGConstraints{}

// Name returns name of the plugin.
func (pl *DAGConstraints) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	klog.V(2).InfoS("DAGConstraints plugin initialized")
	return &DAGConstraints{handle: h}, nil
}

// Filter checks if the node is allowed for this pod based on DAG constraints.
// This is called for each node during the filtering phase.
func (pl *DAGConstraints) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	// Check if this pod has DAG constraints annotation
	allowedNodesStr, exists := pod.Annotations["dag.example.com/allowed-nodes"]
	if !exists {
		// No DAG constraints, allow all nodes
		klog.V(4).InfoS("No DAG constraints for pod, allowing all nodes", "pod", pod.Name)
		return framework.NewStatus(framework.Success, "")
	}

	// Parse the allowed nodes
	allowedNodes := strings.Split(allowedNodesStr, ",")
	for i := range allowedNodes {
		allowedNodes[i] = strings.TrimSpace(allowedNodes[i])
	}

	nodeName := nodeInfo.Node().Name

	// Check if this node is in the allowed list
	for _, allowed := range allowedNodes {
		if nodeName == allowed {
			klog.V(4).InfoS("Node allowed for pod", "pod", pod.Name, "node", nodeName)
			return framework.NewStatus(framework.Success, "")
		}
	}

	klog.V(4).InfoS("Node not allowed for pod", "pod", pod.Name, "node", nodeName, "allowedNodes", allowedNodes)
	return framework.NewStatus(framework.Unschedulable, "node not in allowed list for DAG step")
}

// Score scores nodes based on DAG constraints.
// Currently returns equal scores for all allowed nodes.
func (pl *DAGConstraints) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	// For now, all allowed nodes get the same score
	// Future: could prioritize nodes based on data locality
	return 100, framework.NewStatus(framework.Success, "")
}

// ScoreExtensions returns the score extensions for this plugin.
func (pl *DAGConstraints) ScoreExtensions() framework.ScoreExtensions {
	return nil
}
