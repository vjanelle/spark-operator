/*
Copyright 2025 The Kubeflow authors.

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

package features

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
)

var _ = It("DefaultFeatureGatesRegistered", func() {
	// Verify that defaultFeatureGates is registered with the global feature gate.
	// This test ensures the init() function ran successfully.
	Expect(utilfeature.DefaultFeatureGate).NotTo(BeNil())
})

var _ = It("EnabledWithRegisteredFeature", func() {
	// Register a test feature for this test
	testFeature := featuregate.Feature("TestFeatureForEnabled")
	err := utilfeature.DefaultMutableFeatureGate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		testFeature: {Default: false, PreRelease: featuregate.Alpha},
	})
	Expect(err).NotTo(HaveOccurred())

	// Test that Enabled returns false for a disabled feature
	Expect(Enabled(testFeature)).To(BeFalse())
})

var _ = It("SetEnableWithRegisteredFeature", func() {
	// Register a test feature for this test
	testFeature := featuregate.Feature("TestFeatureForSetEnable")
	err := utilfeature.DefaultMutableFeatureGate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		testFeature: {Default: false, PreRelease: featuregate.Alpha},
	})
	Expect(err).NotTo(HaveOccurred())

	// Test SetEnable
	err = SetEnable(testFeature, true)
	Expect(err).NotTo(HaveOccurred())
	Expect(Enabled(testFeature)).To(BeTrue())

	// Disable the feature
	err = SetEnable(testFeature, false)
	Expect(err).NotTo(HaveOccurred())
	Expect(Enabled(testFeature)).To(BeFalse())
})

var _ = It("DefaultTimeToLiveGateRegisteredAndOffByDefault", func() {
	// The gate must be registered (init ran) and default to disabled.
	Expect(Enabled(DefaultTimeToLive)).To(BeFalse())

	SetFeatureGateDuringTest(GinkgoTB(), DefaultTimeToLive, true)
	Expect(Enabled(DefaultTimeToLive)).To(BeTrue())
})

var _ = It("SetFeatureGateDuringTestHelper", func() {
	// Register a test feature for this test
	testFeature := featuregate.Feature("TestFeatureForDuringTest")
	err := utilfeature.DefaultMutableFeatureGate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		testFeature: {Default: false, PreRelease: featuregate.Alpha},
	})
	Expect(err).NotTo(HaveOccurred())

	// Verify the feature is disabled by default
	Expect(Enabled(testFeature)).To(BeFalse())

	// Enable the feature gate during the test
	SetFeatureGateDuringTest(GinkgoTB(), testFeature, true)

	// Verify the feature is now enabled
	Expect(Enabled(testFeature)).To(BeTrue())

	// After the test, the feature gate will be automatically restored to its original value
})
