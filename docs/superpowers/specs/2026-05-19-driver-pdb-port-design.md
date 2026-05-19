# Per-Application Driver PodDisruptionBudget

**Status:** design approved, implementation pending
**Date:** 2026-05-19
**Source:** ports the per-`SparkApplication` driver PDB feature from the Salesforce fork at `../../spark-on-k8s-operator` (branch `release-2.1`)

## Goal

Allow a `SparkApplication` to opt its driver pod into a Kubernetes `PodDisruptionBudget` so node drains, autoscaler evictions, and other voluntary disruptions cannot evict the driver while the app is running. Eviction of the driver terminates the whole Spark application, so this protection meaningfully reduces job loss during cluster maintenance.

The fork has shipped this in production. Upstream does not. We are porting it with one deliberate divergence: every default is **off** so installing or upgrading the operator changes no behavior unless an operator and a user both explicitly opt in.

## Non-goals

- Operator-deployment PDBs (the chart's controller/webhook deployment PDBs and the Kustomize `config/pdb/` overlay) - those already exist upstream and need no change.
- Configurable `MinAvailable` / `MaxUnavailable` on the per-app PDB. The fork hard-codes `MinAvailable=1` and we mirror that.
- Webhook-side defaulting that flips PDBs on automatically. The fork does this when its operator-level gate is enabled; we drop the behavior entirely (see Default model).
- Canary-labeling defaulter changes, `sf-spark-submitter` adapter, and other fork features that happen to share commits with the PDB work.

## Default model

A driver PDB is created if and only if **both** of:

1. The controller binary was started with `--enable-driver-pdb` (default `false`), and
2. The `SparkApplication` sets `spec.driverPodDisruptionBudget.enabled: true` explicitly.

This means:

| Layer | Default | Opt-in |
|---|---|---|
| Controller `--enable-driver-pdb` flag | `false` | Helm value or Kustomize patch sets it on the controller deployment |
| `SparkApplication.spec.driverPodDisruptionBudget` | `nil` | User adds the field |
| `DriverPDBSpec.Enabled` Go zero value | `false` | User sets `enabled: true` |
| Webhook defaulter | does not touch the field | n/a |

Diverges from the fork on two points, and both divergences are intentional:

- The fork's CRD has `+kubebuilder:default=true` on `Enabled`, so `driverPodDisruptionBudget: {}` becomes "on". We drop the marker so the field's zero value is `false`. Empty-struct submissions stay off.
- The fork's webhook defaulter, when its mirrored gate is on, injects `&DriverPDBSpec{Enabled: true}` for any app whose field is nil. We drop that mutation. The webhook never writes to this field; users who want a PDB set `enabled: true` themselves. As a consequence the webhook does not need `--enable-driver-pdb` and we do not wire that flag on the webhook binary.

This keeps the operator gate as a single permission point on the controller side and removes the cross-binary coupling that exists in the fork.

## API

In `api/v1beta2/sparkapplication_types.go`, add to `SparkApplicationSpec`:

```go
// DriverPodDisruptionBudget prevents voluntary eviction of the driver pod
// during cluster operations.
// +optional
DriverPodDisruptionBudget *DriverPDBSpec `json:"driverPodDisruptionBudget,omitempty"`
```

And the new type at the bottom of the file:

```go
// DriverPDBSpec defines the Pod Disruption Budget configuration for the driver pod.
type DriverPDBSpec struct {
    // Enabled controls whether a PodDisruptionBudget is created for the driver pod.
    // The operator must also be started with --enable-driver-pdb for the PDB to be created.
    Enabled bool `json:"enabled"`
}
```

Regenerate (`make generate update-crd build-api-docs`):
- `api/v1beta2/zz_generated.deepcopy.go`
- `api/v1beta2/zz_generated.openapi.go`
- `config/crd/bases/sparkoperator.k8s.io_sparkapplications.yaml`
- `charts/spark-operator-chart/crds/sparkoperator.k8s.io_sparkapplications.yaml` (via `make update-crd`)
- `api/openapi-spec/`, `api/python_api/`
- `docs/api-docs.md`

`make detect-crds-drift` must stay clean.

## Reconciler

A new file `internal/controller/sparkapplication/driver_pdb.go` carries three small methods on `*Reconciler`:

```go
// createDriverPDBObject builds a MinAvailable=1 PDB selecting the driver pod.
// It does not talk to the API server; it only constructs the object.
func (r *Reconciler) createDriverPDBObject(app *v1beta2.SparkApplication) *policyv1.PodDisruptionBudget

// createDriverPDB creates the PDB if both gates are open. AlreadyExists is
// treated as success. Errors are returned to the caller, which logs and
// continues.
func (r *Reconciler) createDriverPDB(ctx context.Context, app *v1beta2.SparkApplication) error

// deleteDriverPDB deletes the PDB if both gates are open. NotFound is success.
// Both gates are checked here too so the create and delete paths stay
// symmetric - past fork bug 64a9f0977 traced to gate drift between the two.
func (r *Reconciler) deleteDriverPDB(ctx context.Context, app *v1beta2.SparkApplication) error
```

Object shape:

- `Name`: `util.GetDriverPodName(app)` (matches the driver pod's name).
- `Namespace`: app's namespace.
- `Spec.MinAvailable`: `intstr.FromInt(1)`.
- `Spec.Selector.MatchLabels`: `spark-app-name=<app-name>` and `spark-role=driver` (constants in `pkg/common/`).
- `OwnerReferences`: set via `ctrl.SetControllerReference(app, pdb, r.scheme)` so the API server garbage-collects the PDB if the `SparkApplication` is deleted before the reconciler's normal cleanup runs.

Both methods short-circuit identically:

```go
if !r.options.EnableDriverPDB {
    return nil
}
if app.Spec.DriverPodDisruptionBudget == nil || !app.Spec.DriverPodDisruptionBudget.Enabled {
    return nil
}
```

In `controller.go`:

1. Add `EnableDriverPDB bool` to `Options`.
2. Add the kubebuilder RBAC marker near the existing markers (around line 111 of the upstream file):
   ```go
   // +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
   ```
3. In `submitSparkApplication`, immediately after a successful submitter call, invoke `r.createDriverPDB(ctx, app)`. Log on error; do not fail the submission. The PDB only matters once the driver is running.
4. In `deleteSparkResources`, call `r.deleteDriverPDB(ctx, app)` alongside the existing pod / service / ingress cleanup.

The owner-reference safety net means we are not relying on `deleteSparkResources` running for cleanup; if a `SparkApplication` is force-deleted, the PDB still goes away.

In `internal/controller/sparkapplication/suite_test.go`, register the policy scheme:

```go
import policyv1 "k8s.io/api/policy/v1"
...
err = policyv1.AddToScheme(scheme.Scheme)
Expect(err).NotTo(HaveOccurred())
```

## Webhook

No changes to the webhook defaulter or its constructor. `SparkApplicationDefaulter` keeps the upstream signature (`NewSparkApplicationDefaulter()`); the webhook binary does not learn about `--enable-driver-pdb`. Validation is unchanged: an explicit `Enabled: true` is always a legal value, the operator just won't act on it unless the controller gate is on.

## Flag wiring

`cmd/operator/controller/start.go`:

```go
var enableDriverPDB bool

command.Flags().BoolVar(&enableDriverPDB, "enable-driver-pdb", false,
    "Enable creation of a PodDisruptionBudget for Spark driver pods. "+
        "Each SparkApplication must additionally opt in via spec.driverPodDisruptionBudget.enabled.")
...
options := sparkapplication.Options{
    ...
    EnableDriverPDB: enableDriverPDB,
}
```

`cmd/operator/webhook/start.go`: no change.

## Deployment artifacts

Both Helm and Kustomize must surface the controller flag. Default off in both.

**Helm** (`charts/spark-operator-chart/values.yaml`):

```yaml
controller:
  ...
  driverPodDisruptionBudget:
    # When true, the controller creates a PodDisruptionBudget for each
    # SparkApplication that sets spec.driverPodDisruptionBudget.enabled=true.
    enable: false
```

In `templates/controller/deployment.yaml`, conditionally append `--enable-driver-pdb=true` to the controller's args when the value is true. (Default-off means not appending the flag at all is equivalent to passing `--enable-driver-pdb=false`; we prefer the conditional so the rendered manifest matches the chart's stated configuration.)

**Kustomize** (`config/manager/manager.yaml`): no change to the base. Documented in the deploy guide that operators wanting the feature add a strategic-merge patch appending `--enable-driver-pdb=true` to the controller container's args.

`README.md` for the chart, `helm-docs` regeneration, and the chart README test snapshot all need to capture the new value.

## Tests

Port from the fork (paths, copyright headers, and the `/v2` module suffix updated):

- `internal/controller/sparkapplication/driver_pdb_object_test.go` - table tests over the constructed PDB: name, namespace, selector labels, `MinAvailable=1`, owner references.
- `internal/controller/sparkapplication/driver_pdb_create_delete_test.go` - the gate matrix, expressed as a table:
  - operator gate off, spec nil -> no PDB
  - operator gate off, spec `{Enabled: true}` -> no PDB
  - operator gate on, spec nil -> no PDB (this is the divergence from the fork; the fork's defaulter would have populated the spec by this point, ours does not)
  - operator gate on, spec `{Enabled: false}` -> no PDB
  - operator gate on, spec `{Enabled: true}` -> PDB created
  - second create call when PDB already exists -> no error (`AlreadyExists` is treated as success)
  - delete when no PDB exists -> no error (`NotFound` is treated as success)
  - delete on a path where create wouldn't have run -> no API call
- `internal/controller/sparkapplication/driver_pdb_test.go` - Ginkgo specs that drive the reconciler end-to-end through envtest, asserting create on submit and delete on terminal-state cleanup.
- `test/e2e/pdb_test.go` - apply a `SparkApplication` with the field on while the operator runs with the flag on; assert the PDB exists; delete the app and assert the PDB is collected. Then repeat with the field off and assert no PDB is ever created.

We are intentionally not porting the fork's defaulter test for this feature - there is no defaulter behavior to test on our side.

## Validation order

1. `make generate update-crd build-api-docs` and commit every regenerated file.
2. `make go-fmt go-vet go-lint`.
3. `make unit-test` (covers reconciler envtest specs).
4. `make helm-unittest helm-lint helm-docs`.
5. `make kustomize-lint detect-crds-drift`.
6. `make kind-create-cluster kind-load-image && make e2e-test DEPLOY_METHOD=helm`.

## Open questions

None pending design approval.
