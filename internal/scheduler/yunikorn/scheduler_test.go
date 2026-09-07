/*
Copyright 2024 The Kubeflow authors.

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

package yunikorn

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

var _ = Describe("Schedule", func() {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected []taskGroup
	}{
		{
			name: "spark-pi-yunikorn",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:     ptr.To[int32](1),
							CoreLimit: ptr.To("1200m"),
							Memory:    ptr.To("512m"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:     ptr.To[int32](1),
							CoreLimit: ptr.To("1200m"),
							Memory:    ptr.To("512m"),
						},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: ptr.To("root.default"),
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "896Mi", // 512Mi + 384Mi min overhead
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 2,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "896Mi", // 512Mi + 384Mi min overhead
					},
				},
			},
		},
		{
			name: "Dynamic allocation and memory overhead",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypePython,
					MemoryOverheadFactor: ptr.To("0.3"),
					Driver: v1beta2.DriverSpec{
						CoreRequest: ptr.To("2000m"),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](4),
							Memory: ptr.To("8g"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](4),
						SparkPodSpec: v1beta2.SparkPodSpec{
							MemoryOverhead: ptr.To("2g"),
							Cores:          ptr.To[int32](8),
							Memory:         ptr.To("64g"),
						},
					},
					DynamicAllocation: &v1beta2.DynamicAllocation{
						Enabled:          true,
						InitialExecutors: ptr.To[int32](8),
						MinExecutors:     ptr.To[int32](2),
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: ptr.To("root.default"),
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "2000m",   // CoreRequest takes precedence over Cores
						"memory": "10649Mi", // 1024Mi * 8 * 1.3 (manually specified overhead)
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 8, // Max of instances, dynamic allocation min and initial
					MinResource: map[string]string{
						"cpu":    "8",
						"memory": "67584Mi", // 1024Mi * 64 + 1024 * 2 (executor memory overhead takes precedence)
					},
				},
			},
		},
		{
			name: "Node selectors, tolerations, affinity and labels",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:         v1beta2.SparkApplicationTypePython,
					NodeSelector: map[string]string{"key": "value"},
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:        ptr.To[int32](1),
							Memory:       ptr.To("1g"),
							NodeSelector: map[string]string{"key": "newvalue", "key2": "value2"},
							Tolerations: []corev1.Toleration{
								{
									Key:      "example-key",
									Operator: corev1.TolerationOpEqual,
									Value:    "example-value",
									Effect:   corev1.TaintEffectNoSchedule,
								},
							},
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](1),
							Memory: ptr.To("1g"),
							Affinity: &corev1.Affinity{
								NodeAffinity: &corev1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
										NodeSelectorTerms: []corev1.NodeSelectorTerm{
											{
												MatchExpressions: []corev1.NodeSelectorRequirement{
													{
														Key:      "another-key",
														Operator: corev1.NodeSelectorOpIn,
														Values:   []string{"value1", "value2"},
													},
												},
											},
										},
									},
								},
							},
							Labels: map[string]string{"label": "value"},
						},
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "1433Mi", // 1024Mi * 1.4 non-JVM overhead
					},
					NodeSelector: map[string]string{"key": "newvalue", "key2": "value2"},
					Tolerations: []corev1.Toleration{
						{
							Key:      "example-key",
							Operator: corev1.TolerationOpEqual,
							Value:    "example-value",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "1433Mi", // 1024Mi * 1.4 non-JVM overhead
					},
					NodeSelector: map[string]string{"key": "value"}, // No executor specific node-selector
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
								NodeSelectorTerms: []corev1.NodeSelectorTerm{
									{
										MatchExpressions: []corev1.NodeSelectorRequirement{
											{
												Key:      "another-key",
												Operator: corev1.NodeSelectorOpIn,
												Values:   []string{"value1", "value2"},
											},
										},
									},
								},
							},
						},
					},
					Labels: map[string]string{"label": "value"},
				},
			},
		},
		{
			name: "spark.executor.pyspark.memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](1),
							Memory: ptr.To("512m"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](1),
							Memory: ptr.To("512m"),
						},
					},
					SparkConf: map[string]string{
						"spark.executor.pyspark.memory": "500m",
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "896Mi", // 512Mi + 384Mi min overhead
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 2,
					MinResource: map[string]string{
						"cpu": "1",
						// 512Mi + 384Mi min overhead + 500Mi spark.executor.pyspark.memory
						"memory": "1396Mi",
					},
				},
			},
		},
		{
			name: "spark.memory.offHeap.size",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](1),
							Memory: ptr.To("512m"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](1),
							Memory: ptr.To("512m"),
						},
					},
					SparkConf: map[string]string{
						"spark.memory.offHeap.enabled": "true",
						"spark.memory.offHeap.size":    "400m",
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "896Mi", // 512Mi + 384Mi min overhead
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 2,
					MinResource: map[string]string{
						"cpu": "1",
						// 512Mi + 384Mi min overhead + 400Mi spark.memory.offHeap.size
						"memory": "1296Mi",
					},
				},
			},
		},
		{
			name: "spark.memory.offHeap.size and spark.executor.pyspark.memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](1),
							Memory: ptr.To("512m"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  ptr.To[int32](1),
							Memory: ptr.To("512m"),
						},
					},
					SparkConf: map[string]string{
						"spark.memory.offHeap.enabled":  "true",
						"spark.memory.offHeap.size":     "400m",
						"spark.executor.pyspark.memory": "500m",
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "896Mi", // 512Mi + 384Mi min overhead
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 2,
					MinResource: map[string]string{
						"cpu": "1",
						// 512Mi + 384Mi min overhead + 400Mi spark.memory.offHeap.size + 500Mi spark.executor.pyspark.memory
						"memory": "1796Mi",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		It(tc.name, func() {
			tc.app = tc.app.DeepCopy()
			scheduler := &Scheduler{}
			marshalledExpected, err := json.Marshal(tc.expected)
			Expect(err).NotTo(HaveOccurred(), "Failed to marshal expected task groups: %v", err)

			err = scheduler.Schedule(tc.app)
			Expect(err).NotTo(HaveOccurred())
			Expect(tc.app.Spec.Driver.Annotations[taskGroupsAnnotation]).To(MatchJSON(string(marshalledExpected)))

			options := tc.app.Spec.BatchSchedulerOptions
			if options != nil && options.Queue != nil {
				Expect(tc.app.Spec.Driver.Labels[queueLabel]).To(Equal(*options.Queue))
				Expect(tc.app.Spec.Executor.Labels[queueLabel]).To(Equal(*options.Queue))
			}

			Expect(*tc.app.Spec.Driver.SchedulerName).To(Equal("yunikorn"))
			Expect(*tc.app.Spec.Executor.SchedulerName).To(Equal("yunikorn"))
		})
	}
})

var _ = Describe("MergeNodeSelector", func() {
	It("preserves the expected behavior", func() {
		testCases := []struct {
			appNodeSelector map[string]string
			podNodeSelector map[string]string
			expected        map[string]string
		}{
			{
				appNodeSelector: map[string]string{},
				podNodeSelector: map[string]string{},
				expected:        nil,
			},
			{
				appNodeSelector: map[string]string{"key1": "value1"},
				podNodeSelector: map[string]string{},
				expected:        map[string]string{"key1": "value1"},
			},
			{
				appNodeSelector: map[string]string{},
				podNodeSelector: map[string]string{"key1": "value1"},
				expected:        map[string]string{"key1": "value1"},
			},
			{
				appNodeSelector: map[string]string{"key1": "value1"},
				podNodeSelector: map[string]string{"key2": "value2"},
				expected:        map[string]string{"key1": "value1", "key2": "value2"},
			},
			{
				appNodeSelector: map[string]string{"key1": "value1"},
				podNodeSelector: map[string]string{"key1": "value2", "key2": "value2"},
				expected:        map[string]string{"key1": "value2", "key2": "value2"},
			},
		}

		for _, tc := range testCases {
			Expect(mergeNodeSelector(tc.appNodeSelector, tc.podNodeSelector)).To(Equal(tc.expected))
		}
	})
})
