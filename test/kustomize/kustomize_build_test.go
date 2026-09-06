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

package kustomize_test

import (
	"bytes"
	"encoding/json"
	"io"
	"os/exec"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

// buildKustomize runs "kubectl kustomize" on config/default and returns the
// parsed Kubernetes resources. The test is skipped when kubectl is absent.
func buildKustomize(t GinkgoTInterface) []unstructured.Unstructured {
	t.Helper()

	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found in PATH, skipping kustomize build test")
	}

	repoRoot := filepath.Join("..", "..")
	kustomizeDir := filepath.Join(repoRoot, "config", "default")

	cmd := exec.Command("kubectl", "kustomize", kustomizeDir)
	output, err := cmd.CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "kustomize build failed:\n%s", string(output))

	var resources []unstructured.Unstructured
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(output), 4096)
	for {
		obj := unstructured.Unstructured{}
		if err := decoder.Decode(&obj); err != nil {
			if err == io.EOF {
				break
			}
			t.Logf("skipping unparseable document: %v", err)
			continue
		}
		if obj.GetKind() != "" {
			resources = append(resources, obj)
		}
	}

	Expect(resources).NotTo(BeEmpty(), "kustomize build produced no resources")
	return resources
}

// --- helpers ---

func countKind(resources []unstructured.Unstructured, kind string) int {
	n := 0
	for i := range resources {
		if resources[i].GetKind() == kind {
			n++
		}
	}
	return n
}

func findResource(resources []unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	for i := range resources {
		if resources[i].GetKind() == kind && resources[i].GetName() == name {
			return &resources[i]
		}
	}
	return nil
}

func findResources(resources []unstructured.Unstructured, kind string) []unstructured.Unstructured {
	var out []unstructured.Unstructured
	for i := range resources {
		if resources[i].GetKind() == kind {
			out = append(out, resources[i])
		}
	}
	return out
}

func convertTo[T any](t GinkgoTInterface, obj *unstructured.Unstructured) *T {
	t.Helper()
	data, err := json.Marshal(obj.Object)
	Expect(err).NotTo(HaveOccurred())
	result := new(T)
	Expect(json.Unmarshal(data, result)).NotTo(HaveOccurred())
	return result
}

func rulesHaveResource(rules []rbacv1.PolicyRule, resource string) bool {
	for _, rule := range rules {
		for _, r := range rule.Resources {
			if r == resource {
				return true
			}
		}
	}
	return false
}

func rulesHaveResourceName(rules []rbacv1.PolicyRule, name string) bool {
	for _, rule := range rules {
		for _, rn := range rule.ResourceNames {
			if rn == name {
				return true
			}
		}
	}
	return false
}

// --- tests ---

var _ = Describe("KustomizeBuild", func() {
	var resources []unstructured.Unstructured
	BeforeEach(func() { resources = buildKustomize(GinkgoT()) })

	Context("ResourceInventory", func() {
		expected := map[string]int{
			"Namespace":                      1,
			"CustomResourceDefinition":       3,
			"ServiceAccount":                 2,
			"ClusterRole":                    6,
			"ClusterRoleBinding":             2,
			"Role":                           2,
			"RoleBinding":                    2,
			"Deployment":                     2,
			"Service":                        1,
			"MutatingWebhookConfiguration":   1,
			"ValidatingWebhookConfiguration": 1,
		}
		for kind, want := range expected {
			It(kind, func() {
				Expect(countKind(resources, kind)).To(Equal(want), "unexpected %s count", kind)
			})
		}
	})

	It("NamespaceConsistency", func() {
		nsCount := 0
		for i := range resources {
			if resources[i].GetNamespace() == "spark-operator" {
				nsCount++
			}
		}
		Expect(nsCount).To(BeNumerically(">=", 5), "expected at least 5 namespaced resources in spark-operator namespace, got %d", nsCount)

		ns := findResource(resources, "Namespace", "spark-operator")
		Expect(ns).NotTo(BeNil(), "Namespace 'spark-operator' not found")
	})

	It("ImageReplacement", func() {
		t := GinkgoT()
		var imageCount int
		for _, d := range findResources(resources, "Deployment") {
			dep := convertTo[appsv1.Deployment](t, &d)
			for _, c := range dep.Spec.Template.Spec.Containers {
				if strings.HasPrefix(c.Image, "ghcr.io/kubeflow/spark-operator/controller:") {
					imageCount++
				}
			}
		}
		Expect(imageCount).To(Equal(2), "expected 2 upstream image references across deployments")
	})

	It("ControllerRBAC", func() {
		t := GinkgoT()
		crObj := findResource(resources, "ClusterRole", "spark-operator-controller")
		Expect(crObj).NotTo(BeNil(), "ClusterRole 'spark-operator-controller' not found")

		// Leader election lease is scoped in the controller Role, not ClusterRole.
		roleObj := findResource(resources, "Role", "spark-operator-controller")
		Expect(roleObj).NotTo(BeNil(), "Role 'controller' (leader-election) not found")
		role := convertTo[rbacv1.Role](t, roleObj)
		Expect(rulesHaveResourceName(role.Rules, "spark-operator-controller-lock")).To(BeTrue(), "leader election lease should be scoped to 'spark-operator-controller-lock'")

		crbObj := findResource(resources, "ClusterRoleBinding", "spark-operator-controller")
		Expect(crbObj).NotTo(BeNil(), "ClusterRoleBinding 'spark-operator-controller' not found")
		crb := convertTo[rbacv1.ClusterRoleBinding](t, crbObj)

		hasSA := false
		for _, s := range crb.Subjects {
			if s.Kind == "ServiceAccount" {
				hasSA = true
				break
			}
		}
		Expect(hasSA).To(BeTrue(), "controller ClusterRoleBinding should reference a ServiceAccount")
	})

	It("WebhookClusterRole", func() {
		t := GinkgoT()
		obj := findResource(resources, "ClusterRole", "spark-operator-webhook")
		Expect(obj).NotTo(BeNil(), "ClusterRole 'spark-operator-webhook' not found")
		cr := convertTo[rbacv1.ClusterRole](t, obj)

		for _, res := range []string{"pods", "resourcequotas", "sparkapplications", "scheduledsparkapplications", "mutatingwebhookconfigurations"} {
			Expect(rulesHaveResource(cr.Rules, res)).To(BeTrue(), "webhook ClusterRole should have '%s'", res)
		}
		for _, res := range []string{"events"} {
			Expect(rulesHaveResource(cr.Rules, res)).To(BeFalse(), "webhook ClusterRole should NOT have '%s' (not code-required)", res)
		}

		Expect(rulesHaveResourceName(cr.Rules, "mutating-webhook-configuration")).To(BeTrue(), "webhook ClusterRole should scope resourceNames for webhook configs")
	})

	It("WebhookRole", func() {
		t := GinkgoT()
		obj := findResource(resources, "Role", "spark-operator-webhook")
		Expect(obj).NotTo(BeNil(), "Role 'webhook' not found")
		role := convertTo[rbacv1.Role](t, obj)

		Expect(rulesHaveResource(role.Rules, "secrets")).To(BeTrue(), "webhook Role should have 'secrets'")
		Expect(rulesHaveResource(role.Rules, "events")).To(BeTrue(), "webhook Role should have 'events' for leader election event recording")
		Expect(rulesHaveResourceName(role.Rules, "spark-operator-webhook-certs")).To(BeTrue(), "webhook Role should scope secret to 'spark-operator-webhook-certs'")
		Expect(rulesHaveResourceName(role.Rules, "spark-operator-webhook-lock")).To(BeTrue(), "webhook Role should scope lease to 'spark-operator-webhook-lock'")
	})

	It("WebhookConfiguration", func() {
		t := GinkgoT()
		mwObj := findResource(resources, "MutatingWebhookConfiguration", "mutating-webhook-configuration")
		Expect(mwObj).NotTo(BeNil(), "MutatingWebhookConfiguration not found")
		mw := convertTo[admissionregistrationv1.MutatingWebhookConfiguration](t, mwObj)

		hasObjectSelector := false
		for _, wh := range mw.Webhooks {
			if strings.Contains(wh.Name, "mutate-pod") && wh.ObjectSelector != nil {
				hasObjectSelector = true
				break
			}
		}
		Expect(hasObjectSelector).To(BeTrue(), "pod mutation webhook should have objectSelector to prevent chicken-and-egg deadlock")

		for _, wh := range mw.Webhooks {
			Expect(wh.NamespaceSelector).NotTo(BeNil(), "mutating webhook %s should have namespaceSelector (added via kustomize patch)", wh.Name)
		}

		svcRefCount := 0
		for _, wh := range mw.Webhooks {
			if wh.ClientConfig.Service != nil && wh.ClientConfig.Service.Name == "spark-operator-webhook-svc" {
				svcRefCount++
			}
		}

		vwObj := findResource(resources, "ValidatingWebhookConfiguration", "validating-webhook-configuration")
		Expect(vwObj).NotTo(BeNil(), "ValidatingWebhookConfiguration not found")
		vw := convertTo[admissionregistrationv1.ValidatingWebhookConfiguration](t, vwObj)

		for _, wh := range vw.Webhooks {
			Expect(wh.NamespaceSelector).NotTo(BeNil(), "validating webhook %s should have namespaceSelector (added via kustomize patch)", wh.Name)
		}

		for _, wh := range vw.Webhooks {
			if wh.ClientConfig.Service != nil && wh.ClientConfig.Service.Name == "spark-operator-webhook-svc" {
				svcRefCount++
			}
		}
		Expect(svcRefCount).To(BeNumerically(">=", 4), "webhook configs should reference 'spark-operator-webhook-svc' (got %d)", svcRefCount)
	})

	It("DeploymentConfiguration", func() {
		t := GinkgoT()
		deployments := findResources(resources, "Deployment")
		Expect(deployments).To(HaveLen(2), "expected 2 deployments")

		var (
			saNames      []string
			ports        []int32
			readOnlyRoot bool
			runAsNonRoot bool
			hasHealthz   bool
			hasReadyz    bool
		)

		for _, d := range deployments {
			dep := convertTo[appsv1.Deployment](t, &d)
			saNames = append(saNames, dep.Spec.Template.Spec.ServiceAccountName)

			podSC := dep.Spec.Template.Spec.SecurityContext
			if podSC != nil && podSC.RunAsNonRoot != nil && *podSC.RunAsNonRoot {
				runAsNonRoot = true
			}

			for _, c := range dep.Spec.Template.Spec.Containers {
				for _, p := range c.Ports {
					ports = append(ports, p.ContainerPort)
				}
				if c.SecurityContext != nil {
					if c.SecurityContext.ReadOnlyRootFilesystem != nil && *c.SecurityContext.ReadOnlyRootFilesystem {
						readOnlyRoot = true
					}
					if c.SecurityContext.RunAsNonRoot != nil && *c.SecurityContext.RunAsNonRoot {
						runAsNonRoot = true
					}
				}
				if c.LivenessProbe != nil && c.LivenessProbe.HTTPGet != nil && c.LivenessProbe.HTTPGet.Path == "/healthz" {
					hasHealthz = true
				}
				if c.ReadinessProbe != nil && c.ReadinessProbe.HTTPGet != nil && c.ReadinessProbe.HTTPGet.Path == "/readyz" {
					hasReadyz = true
				}
			}
		}

		Expect(saNames).To(ContainElement("spark-operator-controller"), "expected serviceAccountName 'spark-operator-controller'")
		Expect(saNames).To(ContainElement("spark-operator-webhook"), "expected serviceAccountName 'spark-operator-webhook'")
		Expect(ports).To(ContainElement(int32(9443)), "expected containerPort 9443 (webhook)")
		Expect(ports).To(ContainElement(int32(8080)), "expected containerPort 8080 (metrics)")
		Expect(readOnlyRoot).To(BeTrue(), "expected readOnlyRootFilesystem: true")
		Expect(runAsNonRoot).To(BeTrue(), "expected runAsNonRoot: true")
		Expect(hasHealthz).To(BeTrue(), "expected /healthz liveness probe")
		Expect(hasReadyz).To(BeTrue(), "expected /readyz readiness probe")
	})
})
