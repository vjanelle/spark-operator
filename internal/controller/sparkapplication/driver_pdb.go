/*
Copyright 2026 The Kubeflow authors.

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

package sparkapplication

import (
	"context"
	"fmt"

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// createDriverPDBObject builds a MinAvailable=1 PodDisruptionBudget selecting
// the driver pod of the given SparkApplication. It does not contact the API
// server.
func (r *Reconciler) createDriverPDBObject(app *v1beta2.SparkApplication) *policyv1.PodDisruptionBudget {
	pdbName := util.GetDriverPodName(app)
	minAvailable := intstr.FromInt(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pdbName,
			Namespace: app.Namespace,
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					common.LabelSparkAppName: app.Name,
					common.LabelSparkRole:    common.SparkRoleDriver,
				},
			},
		},
	}
}

// ensureDriverPDB converges the driver PDB to the state requested by spec.
// It is the public entry point used by the reconcile loop and is idempotent
// and safe to call on every non-terminal reconcile.
//
// When the operator-level gate (--enable-driver-pdb) is off, this is a no-op:
// flipping the gate off is operator-scoped and intentionally does not GC PDBs
// already protecting running drivers. Existing PDBs left behind in that case
// are reaped at app termination via OwnerReferences GC (and by
// cleanUpOnTermination on the next reconcile of a terminal app, which is also
// gate-guarded).
//
// When the gate is on, the PDB is created if the per-app spec opts in, and
// deleted otherwise. This gives users live convergence for spec flips and
// recovers PDBs that were missed because the gate was off when the app was
// originally submitted.
func (r *Reconciler) ensureDriverPDB(ctx context.Context, app *v1beta2.SparkApplication) error {
	if !r.options.EnableDriverPDB {
		return nil
	}
	if app.Spec.DriverPodDisruptionBudget != nil && *app.Spec.DriverPodDisruptionBudget {
		return r.createDriverPDB(ctx, app)
	}
	return r.deleteDriverPDB(ctx, app)
}

// createDriverPDB creates the driver PDB if both the operator-level gate and
// the per-app spec opt-in are true. AlreadyExists is treated as success.
// Most callers should use ensureDriverPDB; this is the create primitive.
func (r *Reconciler) createDriverPDB(ctx context.Context, app *v1beta2.SparkApplication) error {
	if !r.options.EnableDriverPDB {
		return nil
	}
	if app.Spec.DriverPodDisruptionBudget == nil || !*app.Spec.DriverPodDisruptionBudget {
		return nil
	}

	logger := log.FromContext(ctx)
	pdb := r.createDriverPDBObject(app)

	if err := ctrl.SetControllerReference(app, pdb, r.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference on driver PDB: %v", err)
	}

	logger.Info("Creating driver PodDisruptionBudget", "name", pdb.Name, "namespace", pdb.Namespace)
	if err := r.client.Create(ctx, pdb); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create driver PDB: %v", err)
	}
	return nil
}

// deleteDriverPDB deletes the driver PDB if the operator-level gate is on.
// NotFound is treated as success. Unlike createDriverPDB, this does not gate
// on app.Spec.DriverPodDisruptionBudget: a user flipping the spec from true
// to false (or to nil) on a running app must still drop the existing PDB,
// otherwise it would orphan until the SparkApplication is deleted and kube
// GC runs via OwnerReferences.
func (r *Reconciler) deleteDriverPDB(ctx context.Context, app *v1beta2.SparkApplication) error {
	if !r.options.EnableDriverPDB {
		return nil
	}

	logger := log.FromContext(ctx)
	pdb := r.createDriverPDBObject(app)

	logger.Info("Deleting driver PodDisruptionBudget", "name", pdb.Name, "namespace", pdb.Namespace)
	if err := r.client.Delete(ctx, pdb); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete driver PDB: %v", err)
	}
	return nil
}
