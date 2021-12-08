package main

import (
	"flag"
	"io/ioutil"

	"github.com/sirupsen/logrus"
	klogv1 "k8s.io/klog"
	klogv2 "k8s.io/klog/v2"

	"github.com/openshift/oc-mirror/pkg/cli/mirror"
)

func main() {
	// This attempts to configure klog (used by vendored Kubernetes code) not
	// to log anything.
	// Handle k8s.io/klog
	var fsv1 flag.FlagSet
	klogv1.InitFlags(&fsv1)
	checkErr(fsv1.Set("stderrthreshold", "4"))
	klogv1.SetOutput(ioutil.Discard)
	// Handle k8s.io/klog/v2
	var fsv2 flag.FlagSet
	klogv2.InitFlags(&fsv2)
	checkErr(fsv2.Set("stderrthreshold", "4"))
	klogv2.SetOutput(ioutil.Discard)

	rootCmd := mirror.NewMirrorCmd()
	checkErr(rootCmd.Execute())
}

func checkErr(err error) {
	if err != nil {
		logrus.Fatal(err)
	}
}
