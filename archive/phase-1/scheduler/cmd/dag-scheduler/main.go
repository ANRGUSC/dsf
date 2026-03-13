package main

import (
	"os"

	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	// Import the DAG constraints plugin
	"dag-scheduler/plugins/dagconstraints"
)

func main() {
	// Register our custom plugin
	command := app.NewSchedulerCommand(
		app.WithPlugin(dagconstraints.Name, dagconstraints.New),
	)

	code := cli.Run(command)
	os.Exit(code)
}
