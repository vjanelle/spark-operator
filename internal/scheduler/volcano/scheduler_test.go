/*
Copyright 2019 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volcano

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	fakevolcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("Schedule", func() {
	testCases := []struct {
		name                 string
		app                  *v1beta2.SparkApplication
		expectedQueue        string
		expectedPriorityName string
		expectedMode         string
	}{
		{
			name: "Client mode with queue and priority",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue:             ptr.To("high-priority"),
						PriorityClassName: ptr.To("high"),
					},
				},
			},
			expectedQueue:        "high-priority",
			expectedPriorityName: "high",
			expectedMode:         "client",
		},
		{
			name: "Cluster mode with queue",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-cluster",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: ptr.To("batch-queue"),
					},
				},
			},
			expectedQueue:        "batch-queue",
			expectedPriorityName: "",
			expectedMode:         "cluster",
		},
		{
			name: "Client mode with custom resources",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-resources",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: corev1.ResourceList{
							corev1.ResourceCPU:         resource.MustParse("4"),
							corev1.ResourceMemory:      resource.MustParse("8Gi"),
							corev1.ResourceName("gpu"): resource.MustParse("2"),
						},
					},
				},
			},
			expectedQueue:        "",
			expectedPriorityName: "",
			expectedMode:         "client",
		},
		{
			name: "Client mode with nil BatchSchedulerOptions",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-client-nil-options",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: nil,
				},
			},
			expectedQueue:        "",
			expectedPriorityName: "",
			expectedMode:         "client",
		},
		{
			name: "Cluster mode with custom resources",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-cluster-resources",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](3),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: ptr.To("gpu-queue"),
						Resources: corev1.ResourceList{
							corev1.ResourceCPU:         resource.MustParse("6"),
							corev1.ResourceMemory:      resource.MustParse("12Gi"),
							corev1.ResourceName("gpu"): resource.MustParse("4"),
						},
					},
				},
			},
			expectedQueue:        "gpu-queue",
			expectedPriorityName: "",
			expectedMode:         "cluster",
		},
		{
			name: "Cluster mode with nil BatchSchedulerOptions",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-cluster-nil-options",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: nil,
				},
			},
			expectedQueue:        "",
			expectedPriorityName: "",
			expectedMode:         "cluster",
		},
	}

	for _, tc := range testCases {
		It(tc.name, func() {
			tc.app = tc.app.DeepCopy()
			tc.app.Annotations = make(map[string]string)
			tc.app.Spec.Driver.Annotations = make(map[string]string)
			tc.app.Spec.Executor.Annotations = make(map[string]string)

			var capturedPodGroup *v1beta1.PodGroup
			mockVolcanoClient := fakevolcanoclientset.NewSimpleClientset()

			mockVolcanoClient.PrependReactor("create", "podgroups", func(action clienttesting.Action) (bool, runtime.Object, error) {
				createAction := action.(clienttesting.CreateAction)
				capturedPodGroup = createAction.GetObject().(*v1beta1.PodGroup)
				return false, capturedPodGroup, nil
			})

			scheduler := &Scheduler{
				volcanoClient: mockVolcanoClient,
			}

			err := scheduler.Schedule(tc.app)

			Expect(err).NotTo(HaveOccurred())
			Expect(capturedPodGroup).NotTo(BeNil())

			if tc.expectedQueue != "" {
				Expect(capturedPodGroup.Spec.Queue).To(Equal(tc.expectedQueue))
			}
			if tc.expectedPriorityName != "" {
				Expect(capturedPodGroup.Spec.PriorityClassName).To(Equal(tc.expectedPriorityName))
			}

			switch tc.expectedMode {
			case "client":
				Expect(tc.app.Spec.Executor.Annotations).To(HaveKey(v1beta1.KubeGroupNameAnnotationKey))
				Expect(tc.app.Spec.Driver.Annotations).NotTo(HaveKey(v1beta1.KubeGroupNameAnnotationKey))
			case "cluster":
				Expect(tc.app.Spec.Driver.Annotations).To(HaveKey(v1beta1.KubeGroupNameAnnotationKey))
				Expect(tc.app.Spec.Executor.Annotations).To(HaveKey(v1beta1.KubeGroupNameAnnotationKey))
			}

			expectedPodGroupName := getPodGroupName(tc.app)
			Expect(capturedPodGroup.Name).To(Equal(expectedPodGroupName))

			Expect(capturedPodGroup.OwnerReferences).To(HaveLen(1))
			Expect(capturedPodGroup.OwnerReferences[0].Name).To(Equal(tc.app.Name))
			Expect(capturedPodGroup.OwnerReferences[0].Kind).To(Equal("SparkApplication"))

			// Verify custom resources if specified
			if tc.app.Spec.BatchSchedulerOptions != nil && len(tc.app.Spec.BatchSchedulerOptions.Resources) > 0 {
				Expect(capturedPodGroup.Spec.MinResources).NotTo(BeNil())
				for resourceName, expectedQuantity := range tc.app.Spec.BatchSchedulerOptions.Resources {
					actualQuantity := capturedPodGroup.Spec.MinResources.Name(resourceName, resource.DecimalSI)
					Expect(actualQuantity.Value()).To(Equal(expectedQuantity.Value()), "Resource %s quantity should match in PodGroup MinResources", resourceName)
				}
			}
			if tc.app.Spec.BatchSchedulerOptions == nil {
				Expect(capturedPodGroup.Spec.MinResources).NotTo(BeNil())

				var expectedResources corev1.ResourceList
				if tc.expectedMode == "cluster" {
					// For cluster mode, check that the resources are the sum of driver and executor resources
					driverResources := util.GetDriverRequestResource(tc.app)
					executorResources := util.GetExecutorRequestResource(tc.app)
					expectedResources = util.SumResourceList([]corev1.ResourceList{driverResources, executorResources})
				} else {
					// For client mode, check that only executor resources are used
					expectedResources = util.GetExecutorRequestResource(tc.app)
				}

				for resourceName, expectedQuantity := range expectedResources {
					actualQuantity := capturedPodGroup.Spec.MinResources.Name(resourceName, resource.DecimalSI)
					Expect(actualQuantity.Value()).To(Equal(expectedQuantity.Value()), "Resource %s quantity should match calculated resources", resourceName)
				}
			}
		})
	}
})
