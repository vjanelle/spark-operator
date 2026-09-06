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

package resourceusage

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ByteStringAsMb", func() {
	testCases := []struct {
		input    string
		expected int
	}{
		{"1k", 1024},
		{"1m", 1024 * 1024},
		{"1g", 1024 * 1024 * 1024},
		{"1t", 1024 * 1024 * 1024 * 1024},
		{"1p", 1024 * 1024 * 1024 * 1024 * 1024},
	}

	for _, tc := range testCases {
		It(tc.input, func() {
			actual, err := byteStringAsBytes(tc.input)
			Expect(err).To(BeNil())
			Expect(actual).To(Equal(int64(tc.expected)))
		})
	}
})

var _ = Describe("ByteStringAsMbInvalid", func() {
	invalidInputs := []string{
		"0.064",
		"0.064m",
		"500ub",
		"This breaks 600b",
		"This breaks 600",
		"600gb This breaks",
		"This 123mb breaks",
	}

	for _, input := range invalidInputs {
		It(input, func() {
			_, err := byteStringAsBytes(input)
			Expect(err).NotTo(BeNil())
		})
	}
})
