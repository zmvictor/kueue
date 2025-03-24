# WorkloadPriority Workflow in Kueue

This document explains the overall workflow of how WorkloadPriority is implemented and used in Kueue, covering the full lifecycle from definition to scheduling decisions.

## 1. Definition of Priority Classes

The workflow begins with the definition of `WorkloadPriorityClass` resources. These are Kubernetes custom resources that define priority values for different types of workloads:

```yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: high-priority
spec:
  value: 1000
  description: "For critical workloads that need immediate execution"
```

Administrators can create multiple priority classes with different values to represent the relative importance of different workloads in their organization. Higher values indicate higher priority.

## 2. Association: Jobs Reference Priority Classes via Labels

When users create jobs, they can reference a priority class by adding the `kueue.x-k8s.io/priority-class` label:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: high-priority-job
  labels:
    kueue.x-k8s.io/priority-class: high-priority
spec:
  # Job specification
```

This label associates the job with the corresponding `WorkloadPriorityClass`. The label is defined in the constants package:

```go
// WorkloadPriorityClassLabel is the label key in the workload that holds the
// workloadPriorityClass name.
// This label is always mutable because it might be useful for the preemption.
WorkloadPriorityClassLabel = "kueue.x-k8s.io/priority-class"
```

The label is mutable, allowing users to change the priority of a job after creation if needed, which can be useful for preemption scenarios.

## 3. Extraction: Controllers Extract Priority Class Labels

When a job is created, the corresponding workload controller (e.g., JobController, MPIJobController) creates a Kueue Workload resource. During this process, it extracts the priority class label from the job using the `WorkloadPriorityClassName` function:

```go
func WorkloadPriorityClassName(object client.Object) string {
    if workloadPriorityClassLabel := object.GetLabels()[constants.WorkloadPriorityClassLabel]; workloadPriorityClassLabel != "" {
        return workloadPriorityClassLabel
    }
    return ""
}
```

This function retrieves the priority class name from the job's labels. If no label is found, it returns an empty string, indicating that no priority class is specified.

## 4. Resolution: Priority Values Retrieved from WorkloadPriorityClass

Once the priority class name is extracted, the system needs to resolve the actual priority value. This is done using the `GetPriorityFromWorkloadPriorityClass` function:

```go
func GetPriorityFromWorkloadPriorityClass(ctx context.Context, client client.Client,
    workloadPriorityClass string) (string, string, int32, error) {
    wpc := &kueue.WorkloadPriorityClass{}
    if err := client.Get(ctx, types.NamespacedName{Name: workloadPriorityClass}, wpc); err != nil {
        return "", "", 0, err
    }
    return wpc.Name, constants.WorkloadPriorityClassSource, wpc.Value, nil
}
```

This function:
1. Takes the priority class name as input
2. Retrieves the corresponding `WorkloadPriorityClass` resource from the Kubernetes API server
3. Returns the name, source, and value of the priority class

The resolved priority value is then set in the Workload's `Spec.Priority` field, making it available for scheduling decisions.

## 5. Usage: Scheduler Uses Priority Values for Decision Making

The scheduler uses the priority values in several ways:

### 5.1 Ordering Workloads for Admission

Priority is a key factor in determining the order in which workloads are considered for admission. In both the classical and fair sharing schedulers, priority is used as a secondary ordering criterion:

**Classical Scheduler:**
```go
// 2. Higher priority first if not disabled.
if features.Enabled(features.PrioritySortingWithinCohort) {
    p1 := priority.Priority(a.Obj)
    p2 := priority.Priority(b.Obj)
    if p1 != p2 {
        return p1 > p2
    }
}
```

**Fair Sharing Scheduler:**
```go
// 2: Priority
if features.Enabled(features.PrioritySortingWithinCohort) {
    p1 := priority.Priority(a.Obj)
    p2 := priority.Priority(b.Obj)
    if p1 != p2 {
        return p1 > p2
    }
}
```

In both cases, workloads with higher priority values are considered for admission before those with lower priority values, ensuring that more important workloads are scheduled first.

### 5.2 Preemption Decisions

Priority is also crucial for preemption decisions. When a high-priority workload cannot be scheduled due to resource constraints, the scheduler may decide to preempt (evict) lower-priority workloads to make room.

The preemption policy is configured in the ClusterQueue's specification:

```yaml
preemption:
  withinClusterQueue: PreemptionPolicyLowerPriority
  reclaimWithinCohort: PreemptionPolicyAny
```

When `withinClusterQueue` is set to `PreemptionPolicyLowerPriority`, the scheduler will only preempt workloads with lower priority than the workload being scheduled. This ensures that high-priority workloads can be scheduled even when the cluster is fully utilized by lower-priority workloads.

In the scheduler code, when a workload requires preemption, the system:
1. Identifies workloads that could be preempted to make room
2. Selects the workloads with the lowest priorities first
3. Evicts the selected workloads
4. Attempts to schedule the higher-priority workload with the newly available resources

This is reflected in the scheduler's code where it reserves resources for higher priority workloads:

```go
// we use resourcesToReserve to block capacity up to either the nominal capacity,
// or the borrowing limit when borrowing, so that a lower priority workload cannot
// admit before us.
cq.AddUsage(resourcesToReserve(e, cq))
```

### 5.3 Fairness Calculations

In the fair sharing scheduler, priority is used alongside Dominant Resource Share (DRS) to ensure fair resource allocation while still respecting the relative importance of workloads. The fair sharing algorithm:

1. First considers the Dominant Resource Share (DRS) of each workload
2. For workloads with equal DRS, it considers their priority values
3. For workloads with equal priority, it falls back to FIFO ordering

This approach balances fairness with priority, ensuring that resources are distributed equitably while still giving preference to more important workloads.

## 6. Feature Flag Control

The priority functionality is modular and can be controlled through feature flags. The main flag is `PrioritySortingWithinCohort`, which determines whether priority is used as a sorting criterion:

```go
if features.Enabled(features.PrioritySortingWithinCohort) {
    p1 := priority.Priority(a.Obj)
    p2 := priority.Priority(b.Obj)
    if p1 != p2 {
        return p1 > p2
    }
}
```

This allows administrators to enable or disable priority-based scheduling as needed, providing flexibility in how the system behaves.

## 7. Complete Workflow Summary

The complete workflow of WorkloadPriority in Kueue can be summarized as follows:

1. **Definition**: Administrators define `WorkloadPriorityClass` resources with different priority values
2. **Association**: Users create jobs with labels referencing the appropriate priority classes
3. **Extraction**: Workload controllers extract the priority class labels from jobs
4. **Resolution**: The system resolves the priority values from the referenced priority classes
5. **Application**: The resolved priority values are set in the Workload's `Spec.Priority` field
6. **Scheduling**: The scheduler uses the priority values to:
   - Order workloads for admission (higher priority first)
   - Make preemption decisions (preempt lower priority workloads)
   - Balance fairness with priority in resource allocation

This workflow ensures that workloads are scheduled according to their relative importance, with more critical workloads receiving preferential treatment in resource allocation and scheduling decisions.
