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

package kubescheduler

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

var _ = Describe("FactoryWithValidConfig", func() {
	It("preserves the expected behavior", func() {
		scheme := newTestScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

		cfg := &Config{SchedulerName: Name, Client: fakeClient}
		sch, err := Factory(cfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(sch.Name()).To(Equal(Name))
	})
})

var _ = Describe("FactoryWithInvalidConfig", func() {
	It("preserves the expected behavior", func() {
		_, err := Factory(struct{}{})
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("SchedulerName", func() {
	It("preserves the expected behavior", func() {
		sch, _ := newTestScheduler()
		Expect(sch.Name()).To(Equal(Name))
	})
})

var _ = Describe("ShouldScheduleAlwaysTrue", func() {
	It("preserves the expected behavior", func() {
		sch, _ := newTestScheduler()
		app := newTestSparkApplication()

		Expect(sch.ShouldSchedule(app)).To(BeTrue())
	})
})

var _ = Describe("ScheduleCreatesPodGroupAndLabelsApp", func() {
	It("preserves the expected behavior", func() {
		sch, cl := newTestScheduler()
		app := newTestSparkApplication()

		err := sch.Schedule(app)
		Expect(err).NotTo(HaveOccurred())

		Expect(app.Labels[schedulingv1alpha1.PodGroupLabel]).To(Equal(getPodGroupName(app)))

		created := &schedulingv1alpha1.PodGroup{}
		err = cl.Get(context.Background(), types.NamespacedName{Namespace: app.Namespace, Name: getPodGroupName(app)}, created)
		Expect(err).NotTo(HaveOccurred())

		Expect(created.Spec.MinMember).To(Equal(int32(1)))
		assertResourceListEqual(created.Spec.MinResources, expectedMinResources(app))
		Expect(created.OwnerReferences).To(HaveLen(1))
		Expect(created.OwnerReferences[0].Name).To(Equal(app.Name))
		Expect(created.OwnerReferences[0].Controller).NotTo(BeNil())
		Expect(*created.OwnerReferences[0].Controller).To(BeTrue())
	})
})

var _ = Describe("ScheduleUpdatesExistingPodGroup", func() {
	It("preserves the expected behavior", func() {
		app := newTestSparkApplication()
		existing := &schedulingv1alpha1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:            getPodGroupName(app),
				Namespace:       app.Namespace,
				ResourceVersion: "1",
				Labels:          map[string]string{"existing": "label"},
			},
			Spec: schedulingv1alpha1.PodGroupSpec{
				MinMember: 5,
				MinResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		}

		sch, cl := newTestScheduler(existing)
		app.Labels = map[string]string{"preserve": "me"}

		err := sch.Schedule(app)
		Expect(err).NotTo(HaveOccurred())

		updated := &schedulingv1alpha1.PodGroup{}
		err = cl.Get(context.Background(), types.NamespacedName{Namespace: app.Namespace, Name: getPodGroupName(app)}, updated)
		Expect(err).NotTo(HaveOccurred())

		Expect(updated.Spec.MinMember).To(Equal(int32(1)))
		assertResourceListEqual(updated.Spec.MinResources, expectedMinResources(app))
		Expect(updated.OwnerReferences).To(HaveLen(1))
		Expect(updated.OwnerReferences[0].Name).To(Equal(app.Name))
		Expect(app.Labels["preserve"]).To(Equal("me"))
		Expect(app.Labels[schedulingv1alpha1.PodGroupLabel]).To(Equal(getPodGroupName(app)))
	})
})

var _ = Describe("CleanupDeletesPodGroup", func() {
	It("preserves the expected behavior", func() {
		app := newTestSparkApplication()
		existing := &schedulingv1alpha1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      getPodGroupName(app),
				Namespace: app.Namespace,
			},
		}

		sch, cl := newTestScheduler(existing)

		err := sch.Cleanup(app)
		Expect(err).NotTo(HaveOccurred())

		err = cl.Get(context.Background(), types.NamespacedName{Namespace: app.Namespace, Name: getPodGroupName(app)}, &schedulingv1alpha1.PodGroup{})
		Expect(err).To(HaveOccurred())
		Expect(client.IgnoreNotFound(err) == nil).To(BeTrue())
	})
})

var _ = Describe("CleanupIgnoresNotFound", func() {
	It("preserves the expected behavior", func() {
		sch, _ := newTestScheduler()
		err := sch.Cleanup(newTestSparkApplication())
		Expect(err).NotTo(HaveOccurred())
	})
})

func newTestScheduler(objs ...client.Object) (*Scheduler, client.Client) {
	GinkgoHelper()

	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &Scheduler{name: Name, client: cl}, cl
}

func newTestScheme() *runtime.Scheme {
	GinkgoHelper()

	scheme := runtime.NewScheme()
	Expect(v1beta2.AddToScheme(scheme)).NotTo(HaveOccurred())
	Expect(schedulingv1alpha1.AddToScheme(scheme)).NotTo(HaveOccurred())
	return scheme
}

func newTestSparkApplication() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "test-ns",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:         v1beta2.SparkApplicationTypeScala,
			SparkVersion: "3.5.0",
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:          ptr.To[int32](1),
					Memory:         ptr.To("1Gi"),
					MemoryOverhead: ptr.To("256Mi"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				Instances: ptr.To[int32](2),
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:          ptr.To[int32](1),
					Memory:         ptr.To("2Gi"),
					MemoryOverhead: ptr.To("512Mi"),
				},
			},
		},
	}
}

func expectedMinResources(app *v1beta2.SparkApplication) corev1.ResourceList {
	return util.SumResourceList([]corev1.ResourceList{util.GetDriverRequestResource(app), util.GetExecutorRequestResource(app)})
}

func assertResourceListEqual(actual, expected corev1.ResourceList) {
	GinkgoHelper()
	Expect(actual).To(HaveLen(len(expected)))
	for name, exp := range expected {
		got, ok := actual[name]
		Expect(ok).To(BeTrue(), "missing resource %s", name)
		Expect(exp.Cmp(got)).To(BeZero(), "resource %s mismatch: want %s, got %s", name, exp.String(), got.String())
	}
}
