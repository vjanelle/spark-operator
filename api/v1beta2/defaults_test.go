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
package v1beta2

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = It("SetSparkApplicationDefaultsNilSparkApplicationShouldNotModifySparkApplication", func() {
	var app *SparkApplication

	SetSparkApplicationDefaults(app)

	Expect(app).To(BeNil())
})

var _ = It("SetSparkApplicationDefaultsEmptyModeShouldDefaultToClusterMode", func() {
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	Expect(app.Spec.Mode).To(Equal(DeployModeCluster))
})

var _ = It("SetSparkApplicationDefaultsModeShouldNotChangeIfSet", func() {
	expectedMode := DeployModeClient
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			Mode: expectedMode,
		},
	}

	SetSparkApplicationDefaults(app)

	Expect(app.Spec.Mode).To(Equal(expectedMode))
})

var _ = It("SetSparkApplicationDefaultsEmptyRestartPolicyShouldDefaultToNever", func() {
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	Expect(app.Spec.RestartPolicy.Type).To(Equal(RestartPolicyNever))
})

var _ = It("SetSparkApplicationDefaultsOnFailureRestartPolicyShouldSetDefaultValues", func() {
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			RestartPolicy: RestartPolicy{
				Type: RestartPolicyOnFailure,
			},
		},
	}

	SetSparkApplicationDefaults(app)

	Expect(app.Spec.RestartPolicy.Type).To(Equal(RestartPolicyOnFailure))
	Expect(app.Spec.RestartPolicy.OnFailureRetryInterval).NotTo(BeNil())
	Expect(*app.Spec.RestartPolicy.OnFailureRetryInterval).To(Equal(int64(5)))
	Expect(app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval).NotTo(BeNil())
	Expect(*app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval).To(Equal(int64(5)))
})

var _ = It("SetSparkApplicationDefaultsOnFailureRestartPolicyShouldSetDefaultValueForOnFailureRetryInterval", func() {
	expectedOnSubmissionFailureRetryInterval := int64(14)
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			RestartPolicy: RestartPolicy{
				Type:                             RestartPolicyOnFailure,
				OnSubmissionFailureRetryInterval: &expectedOnSubmissionFailureRetryInterval,
			},
		},
	}

	SetSparkApplicationDefaults(app)

	Expect(app.Spec.RestartPolicy.Type).To(Equal(RestartPolicyOnFailure))
	Expect(app.Spec.RestartPolicy.OnFailureRetryInterval).NotTo(BeNil())
	Expect(*app.Spec.RestartPolicy.OnFailureRetryInterval).To(Equal(int64(5)))
	Expect(app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval).NotTo(BeNil())
	Expect(*app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval).To(Equal(expectedOnSubmissionFailureRetryInterval))
})

var _ = It("SetSparkApplicationDefaultsOnFailureRestartPolicyShouldSetDefaultValueForOnSubmissionFailureRetryInterval", func() {
	expectedOnFailureRetryInterval := int64(10)
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			RestartPolicy: RestartPolicy{
				Type:                   RestartPolicyOnFailure,
				OnFailureRetryInterval: &expectedOnFailureRetryInterval,
			},
		},
	}

	SetSparkApplicationDefaults(app)

	Expect(app.Spec.RestartPolicy.Type).To(Equal(RestartPolicyOnFailure))
	Expect(app.Spec.RestartPolicy.OnFailureRetryInterval).NotTo(BeNil())
	Expect(*app.Spec.RestartPolicy.OnFailureRetryInterval).To(Equal(expectedOnFailureRetryInterval))
	Expect(app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval).NotTo(BeNil())
	Expect(*app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval).To(Equal(int64(5)))
})

var _ = It("SetSparkApplicationDefaultsDriverSpecDefaults", func() {
	//Case1: Driver config not set.
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	if app.Spec.Driver.Cores == nil {
		Fail("Expected app.Spec.Driver.Cores not to be nil.")
	} else {
		Expect(*app.Spec.Driver.Cores).To(Equal(int32(1)))
	}

	if app.Spec.Driver.Memory == nil {
		Fail("Expected app.Spec.Driver.Memory not to be nil.")
	} else {
		Expect(*app.Spec.Driver.Memory).To(Equal("1g"))
	}

	//Case2: Driver config set via SparkConf.
	app = &SparkApplication{
		Spec: SparkApplicationSpec{
			SparkConf: map[string]string{
				"spark.driver.memory": "200M",
				"spark.driver.cores":  "1",
			},
		},
	}
	SetSparkApplicationDefaults(app)

	Expect(app.Spec.Driver.Cores).To(BeNil())
	Expect(app.Spec.Driver.Memory).To(BeNil())
})

var _ = It("SetSparkApplicationDefaultsExecutorSpecDefaults", func() {
	//Case1: Executor config not set.
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	if app.Spec.Executor.Cores == nil {
		Fail("Expected app.Spec.Executor.Cores not to be nil.")
	} else {
		Expect(*app.Spec.Executor.Cores).To(Equal(int32(1)))
	}

	if app.Spec.Executor.Memory == nil {
		Fail("Expected app.Spec.Executor.Memory not to be nil.")
	} else {
		Expect(*app.Spec.Executor.Memory).To(Equal("1g"))
	}

	if app.Spec.Executor.Instances == nil {
		Fail("Expected app.Spec.Executor.Instances not to be nil.")
	} else {
		Expect(*app.Spec.Executor.Instances).To(Equal(int32(1)))
	}

	//Case2: Executor config set via SparkConf.
	app = &SparkApplication{
		Spec: SparkApplicationSpec{
			SparkConf: map[string]string{
				"spark.executor.cores":     "2",
				"spark.executor.memory":    "500M",
				"spark.executor.instances": "3",
			},
		},
	}

	SetSparkApplicationDefaults(app)

	Expect(app.Spec.Executor.Cores).To(BeNil())
	Expect(app.Spec.Executor.Memory).To(BeNil())
	Expect(app.Spec.Executor.Instances).To(BeNil())

	//Case3: Dynamic allocation is enabled with minExecutors = 0
	var minExecs = int32(0)
	app = &SparkApplication{
		Spec: SparkApplicationSpec{
			DynamicAllocation: &DynamicAllocation{
				Enabled:      true,
				MinExecutors: &minExecs,
			},
		},
	}

	SetSparkApplicationDefaults(app)
	Expect(app.Spec.Executor.Instances).To(BeNil())

	//Case4: Dynamic allocation is enabled via SparkConf
	app = &SparkApplication{
		Spec: SparkApplicationSpec{
			SparkConf: map[string]string{
				"spark.dynamicallocation.enabled": "true",
			},
		},
	}

	SetSparkApplicationDefaults(app)
	Expect(app.Spec.Executor.Instances).To(BeNil())
})
