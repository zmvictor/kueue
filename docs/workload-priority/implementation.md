# WorkloadPriority Core Implementation in Kueue

This document explains the core implementation details of the WorkloadPriority system in Kueue, focusing on how priority values are resolved and used in scheduling decisions.

## 1. Priority Resolution

The core function for retrieving a workload's priority is the `Priority()` function in the `pkg/util/priority/priority.go` file:

```go
// Priority returns priority of the given workload.
func Priority(w *kueue.Workload) int32 {
    // When priority of a running workload is nil, it means it was created at a time
    // that there was no global default priority class and the priority class
    // name of the pod was empty. So, we resolve to the static default priority.
    return ptr.Deref(w.Spec.Priority, constants.DefaultPriority)
}
```

This function:
- Takes a workload as input
- Returns the priority value from the workload's `Spec.Priority` field
- If the priority is not set (nil), it returns the default priority value (0) defined in constants

The default priority (0) is used when no priority class is specified or when the default priority class doesn't exist. This ensures that every workload has a priority value for scheduling decisions.

## 2. Priority Class Resolution

When a workload references a WorkloadPriorityClass through its label, the system needs to resolve the actual priority value. This is done using the `GetPriorityFromWorkloadPriorityClass()` function:

```go
// GetPriorityFromWorkloadPriorityClass returns the priority populated from
// workload priority class. If not specified, returns 0.
// DefaultPriority is not called within this function
// because k8s priority class should be checked next.
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
- Takes a context, client, and workload priority class name as input
- Retrieves the WorkloadPriorityClass object from the Kubernetes API server
- Returns the name, source, and value of the priority class, or an error if the class doesn't exist

The function is designed to be part of a chain of priority resolution mechanisms. If the WorkloadPriorityClass doesn't exist, the system can fall back to other priority sources, such as Kubernetes PriorityClass.

## 3. Priority Influence on Scheduling (Classical Iterator)

In the classical scheduling iterator, priority is used as a secondary ordering criterion after considering whether workloads are borrowing resources. This is implemented in the `Less()` method of the `entryOrdering` type in `pkg/scheduler/scheduler.go`:

```go
// Less is the ordering criteria
func (e entryOrdering) Less(i, j int) bool {
    a := e.entries[i]
    b := e.entries[j]

    // 1. Request under nominal quota.
    aBorrows := a.assignment.Borrows()
    bBorrows := b.assignment.Borrows()
    if aBorrows != bBorrows {
        return !aBorrows
    }

    // 2. Higher priority first if not disabled.
    if features.Enabled(features.PrioritySortingWithinCohort) {
        p1 := priority.Priority(a.Obj)
        p2 := priority.Priority(b.Obj)
        if p1 != p2 {
            return p1 > p2
        }
    }

    // 3. FIFO.
    aComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(a.Obj)
    bComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(b.Obj)
    return aComparisonTimestamp.Before(bComparisonTimestamp)
}
```

This method:
- First checks if one workload is borrowing resources and the other isn't (non-borrowing workloads are preferred)
- If both workloads are either borrowing or not borrowing, it checks if priority sorting is enabled
- If priority sorting is enabled, it compares the priorities of the two workloads
- Returns `true` if the first workload has a higher priority (p1 > p2), which means the first workload should be scheduled before the second
- If priorities are equal, it falls back to FIFO ordering based on timestamps

The priority sorting can be disabled through the `PrioritySortingWithinCohort` feature flag, making the system flexible for different scheduling policies.

## 4. Priority in Fair Sharing Scheduling

In the fair sharing scheduler, priority is used as a secondary criterion after Dominant Resource Share (DRS). This is implemented in the `less()` method of the `entryComparer` type in `pkg/scheduler/fair_sharing_iterator.go`:

```go
func (e *entryComparer) less(a, b *entry, parentCohort kueue.CohortReference) bool {
    aDrs := e.drsValues[drsKey{parentCohort: parentCohort, workloadKey: workload.Key(a.Obj)}]
    bDrs := e.drsValues[drsKey{parentCohort: parentCohort, workloadKey: workload.Key(b.Obj)}]
    // 1: DRF
    if aDrs != bDrs {
        return aDrs < bDrs
    }

    // 2: Priority
    if features.Enabled(features.PrioritySortingWithinCohort) {
        p1 := priority.Priority(a.Obj)
        p2 := priority.Priority(b.Obj)
        if p1 != p2 {
            return p1 > p2
        }
    }

    // 3: FIFO
    aComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(a.Obj)
    bComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(b.Obj)
    return aComparisonTimestamp.Before(bComparisonTimestamp)
}
```

This method:
- First compares the Dominant Resource Share (DRS) values of the two workloads (lower DRS is preferred for fairness)
- If DRS values are equal, it checks if priority sorting is enabled
- If priority sorting is enabled, it compares the priorities of the two workloads
- Returns `true` if the first workload has a higher priority (p1 > p2), which means the first workload should be scheduled before the second
- If priorities are equal, it falls back to FIFO ordering based on timestamps

The fair sharing scheduler is designed to balance resource usage across different users or groups, but still respects priority within those constraints.

## 5. Priority in Preemption Decisions

Priority also plays a crucial role in preemption decisions. When a high-priority workload cannot be scheduled due to resource constraints, the scheduler may decide to preempt (evict) lower-priority workloads to make room.

The preemption policy is configured in the ClusterQueue's specification:

```yaml
preemption:
  withinClusterQueue: PreemptionPolicyLowerPriority
  reclaimWithinCohort: PreemptionPolicyAny
```

When `withinClusterQueue` is set to `PreemptionPolicyLowerPriority`, the scheduler will only preempt workloads with lower priority than the workload being scheduled. This ensures that high-priority workloads can be scheduled even when the cluster is fully utilized by lower-priority workloads.

The preemption mechanism:
1. Identifies workloads that could be preempted to make room for the higher-priority workload
2. Selects the workloads with the lowest priorities first
3. Evicts the selected workloads
4. Attempts to schedule the higher-priority workload with the newly available resources

This preemption capability is a key benefit of using the priority system, as it allows critical workloads to be scheduled even in resource-constrained environments.
