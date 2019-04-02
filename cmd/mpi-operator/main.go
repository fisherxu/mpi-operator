// Copyright 2018 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	restclientset "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	election "k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/sample-controller/pkg/signals"

	"github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v1alpha1"
	clientset "github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned"
	"github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/kubeflow/mpi-operator/pkg/client/informers/externalversions"
	"github.com/kubeflow/mpi-operator/pkg/controllers"
)

var (
	masterURL              string
	kubeConfig             string
	gpusPerNode            int
	processingUnitsPerNode int
	processingResourceType string
	kubectlDeliveryImage   string
	namespace              string
	enableGangScheduling   bool
)

const (
	// leader election config
	leaseDuration = 15 * time.Second
	renewDuration = 5 * time.Second
	retryPeriod   = 3 * time.Second
)

func main() {
	flag.Parse()

	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeConfig)
	if err != nil {
		glog.Fatalf("Error building kubeConfig: %s", err.Error())
	}

	kubeClient, leaderElectionClientSet, kubeflowClient, err := createClientSets(cfg)
	if err != nil {
		glog.Fatalf("Error building clientset: %s", err.Error())
	}

	var kubeInformerFactory kubeinformers.SharedInformerFactory
	var kubeflowInformerFactory informers.SharedInformerFactory
	if namespace == "" {
		kubeInformerFactory = kubeinformers.NewSharedInformerFactory(kubeClient, 0)
		kubeflowInformerFactory = informers.NewSharedInformerFactory(kubeflowClient, 0)
	} else {
		kubeInformerFactory = kubeinformers.NewSharedInformerFactoryWithOptions(kubeClient, 0, kubeinformers.WithNamespace(namespace), nil)
		kubeflowInformerFactory = informers.NewSharedInformerFactoryWithOptions(kubeflowClient, 0, informers.WithNamespace(namespace), nil)
	}

	controller := controllers.NewMPIJobController(
		kubeClient,
		kubeflowClient,
		kubeInformerFactory.Core().V1().ConfigMaps(),
		kubeInformerFactory.Core().V1().ServiceAccounts(),
		kubeInformerFactory.Rbac().V1().Roles(),
		kubeInformerFactory.Rbac().V1().RoleBindings(),
		kubeInformerFactory.Apps().V1().StatefulSets(),
		kubeInformerFactory.Batch().V1().Jobs(),
		kubeInformerFactory.Policy().V1beta1().PodDisruptionBudgets(),
		kubeflowInformerFactory.Kubeflow().V1alpha1().MPIJobs(),
		gpusPerNode,
		processingUnitsPerNode,
		processingResourceType,
		kubectlDeliveryImage,
		enableGangScheduling)

	go kubeInformerFactory.Start(stopCh)
	go kubeflowInformerFactory.Start(stopCh)

	// Set leader election start function.
	run := func(ctx context.Context) {
		if err = controller.Run(2, ctx.Done()); err != nil {
			glog.Fatalf("Error running controller: %s", err.Error())
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	id, err := os.Hostname()
	if err != nil {
		glog.Fatalf("failed to get hostname: %v", err)
	}

	// Prepare event clients.
	eventBroadcaster := record.NewBroadcaster()
	if err = v1.AddToScheme(scheme.Scheme); err != nil {
		glog.Fatalf("CoreV1 Add Scheme failed: %v", err)
	}
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "mpi-operator"})

	namespace := os.Getenv(v1alpha1.EnvKubeflowNamespace)
	if len(namespace) == 0 {
		glog.Infof("EnvKubeflowNamespace not set, use default namespace")
		namespace = metav1.NamespaceDefault
	}

	rl := &resourcelock.EndpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "mpi-operator",
		},
		Client: leaderElectionClientSet.CoreV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
	}

	// Start leader election.
	election.RunOrDie(ctx, election.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDuration,
		RetryPeriod:   retryPeriod,
		Callbacks: election.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				glog.Fatalf("leader election lost")
			},
		},
	})
}

func init() {
	flag.StringVar(&kubeConfig, "kubeConfig", "", "Path to a kubeConfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeConfig. Only required if out-of-cluster.")
	flag.IntVar(
		&gpusPerNode,
		"gpus-per-node",
		1,
		"(Deprecated. This will be overwritten by MPIJobSpec) The maximum number of GPUs available per node. Note that this will be ignored if the GPU resources are explicitly specified in the MPIJob pod spec.")
	flag.StringVar(&kubectlDeliveryImage, "kubectl-delivery-image", "", "The container image used to deliver the kubectl binary.")
	flag.StringVar(&namespace, "namespace", "", "The namespace used to obtain the listers.")
	flag.IntVar(
		&processingUnitsPerNode,
		"processing-units-per-node",
		1,
		"(Deprecated. This will be overwritten by MPIJobSpec) The maximum number of processing units available per node. Note that this will be ignored if the processing resources are explicitly specified in the MPIJob pod spec.")
	flag.StringVar(&processingResourceType, "processing-resource-type", "nvidia.com/gpu", "(Deprecated. This will be overwritten by MPIJobSpec) The compute resource name, e.g. 'nvidia.com/gpu' or 'cpu'.")
	flag.BoolVar(&enableGangScheduling, "enable-gang-scheduling", false, "Whether to enable gang scheduling by kube-batch.")
}

func createClientSets(config *restclientset.Config) (kubernetes.Interface, kubernetes.Interface, clientset.Interface, error) {
	kubeClientSet, err := kubernetes.NewForConfig(restclientset.AddUserAgent(config, "mpi-operator"))
	if err != nil {
		return nil, nil, nil, err
	}

	leaderElectionClientSet, err := kubernetes.NewForConfig(restclientset.AddUserAgent(config, "leader-election"))
	if err != nil {
		return nil, nil, nil, err
	}

	mpiJobClientSet, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, nil, nil, err
	}

	return kubeClientSet, leaderElectionClientSet, mpiJobClientSet, nil
}
