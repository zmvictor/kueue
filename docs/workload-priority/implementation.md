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

Priority plays a crucial role in preemption decisions. When a high-priority workload cannot be scheduled due to resource constraints, the scheduler may decide to preempt (evict) lower-priority workloads to make room. The preemption logic is primarily implemented in the `pkg/scheduler/preemption` package.

### 5.1 Preemption Policies

ClusterQueues define preemption policies in their specification:

```yaml
preemption:
  withinClusterQueue: PreemptionPolicyLowerPriority
  reclaimWithinCohort: PreemptionPolicyAny
  borrowWithinCohort:
    policy: LowerPriority
    maxPriorityThreshold: 0
```

Kueue supports several preemption policies:

- `Never`: No preemption is allowed
- `LowerPriority`: Only workloads with lower priority than the pending workload can be preempted
- `LowerOrNewerEqualPriority`: Workloads with lower priority or equal priority but created more recently can be preempted
- `Any`: Any workload can be preempted regardless of priority

These policies can be applied in different contexts:

- `withinClusterQueue`: Controls preemption within a single ClusterQueue
- `reclaimWithinCohort`: Controls preemption across ClusterQueues in a cohort
- `borrowWithinCohort`: Controls preemption while borrowing resources

### 5.2 How Priority Influences Candidate Selection

The `findCandidates` method in `preemption.go` selects potential workloads for preemption based on their priority:

```go
func (p *Preemptor) findCandidates(wl *kueue.Workload, cq *cache.ClusterQueueSnapshot, frsNeedPreemption sets.Set[resources.FlavorResource]) []*workload.Info {
    var candidates []*workload.Info
    wlPriority := priority.Priority(wl)

    if cq.Preemption.WithinClusterQueue != kueue.PreemptionPolicyNever {
        considerSamePrio := (cq.Preemption.WithinClusterQueue == kueue.PreemptionPolicyLowerOrNewerEqualPriority)
        preemptorTS := p.workloadOrdering.GetQueueOrderTimestamp(wl)

        for _, candidateWl := range cq.Workloads {
            candidatePriority := priority.Priority(candidateWl.Obj)
            if candidatePriority > wlPriority {
                continue
            }

            if candidatePriority == wlPriority && !(considerSamePrio && preemptorTS.Before(p.workloadOrdering.GetQueueOrderTimestamp(candidateWl.Obj))) {
                continue
            }
            // Additional checks...
            candidates = append(candidates, candidateWl)
        }
    }
    // Additional checks for cohort preemption...
    return candidates
}
```

This function:
1. Gets the priority of the incoming workload
2. Based on the preemption policy, filters workloads that can be preempted
3. For `LowerPriority` policy, only workloads with strictly lower priority are considered
4. For `LowerOrNewerEqualPriority`, workloads with equal priority but created after the incoming workload are also considered

### 5.3 Candidate Ordering

Once candidates are selected, they are ordered using the `candidatesOrdering` function:

```go
func candidatesOrdering(candidates []*workload.Info, cq kueue.ClusterQueueReference, now time.Time) func(int, int) bool {
    return func(i, j int) bool {
        a := candidates[i]
        b := candidates[j]
        // Other criteria...
        pa := priority.Priority(a.Obj)
        pb := priority.Priority(b.Obj)
        if pa != pb {
            return pa < pb
        }
        // Additional ordering criteria...
    }
}
```

The ordering criteria are:
1. Already evicted workloads first
2. Workloads from other ClusterQueues in the cohort before ones in the same ClusterQueue
3. **Workloads with lower priority first**
4. More recently admitted workloads first

This ensures that when selecting workloads to preempt, lower priority workloads are chosen before higher priority ones, which aligns with the goal of priority-based scheduling.

### 5.4 Minimal Preemption Algorithm

The core preemption algorithm is implemented in the `minimalPreemptions` function, which tries to find the minimal set of workloads to preempt:

```go
func minimalPreemptions(preemptionCtx *preemptionCtx, candidates []*workload.Info, allowBorrowing bool, allowBorrowingBelowPriority *int32) []*Target {
    // Simulate removing all candidates from the ClusterQueue and cohort.
    var targets []*Target
    fits := false
    for _, candWl := range candidates {
        // Determine reason for preemption based on ClusterQueue and priority
        preemptionCtx.snapshot.RemoveWorkload(candWl)
        targets = append(targets, &Target{
            WorkloadInfo: candWl,
            Reason:       reason,
        })
        if workloadFits(preemptionCtx, allowBorrowing) {
            fits = true
            break
        }
    }
    
    // If we can't fit even after removing all candidates, restore and return nil
    if !fits {
        restoreSnapshot(preemptionCtx.snapshot, targets)
        return nil
    }
    
    // Try to add workloads back while still fitting
    targets = fillBackWorkloads(preemptionCtx, targets, allowBorrowing)
    return targets
}
```

This algorithm:
1. Simulates removing candidates one by one, starting with the lowest priority
2. Once the incoming workload fits, it stops removing candidates
3. Then tries to add back candidates in reverse order, as long as the incoming workload still fits
4. This ensures that only the minimal set of workloads is preempted

### 5.5 Preemption Execution

When preemption targets are identified, the `IssuePreemptions` method marks them for eviction:

```go
func (p *Preemptor) IssuePreemptions(ctx context.Context, preemptor *workload.Info, targets []*Target) (int, error) {
    // For each target workload
    for _, target := range targets {
        message := preemptionMessage(preemptor.Obj, target.Reason)
        err := p.applyPreemption(ctx, target.WorkloadInfo.Obj, target.Reason, message)
        // Record events and metrics
    }
    return successfullyPreempted, nil
}
```

The preempted workloads are marked with an `Evicted` condition and a `Preempted` condition that includes:
- The UID of the preempting workload
- The reason for preemption (e.g., "prioritization in the ClusterQueue")
- A human-readable message explaining the preemption

This preemption capability is a key benefit of using the priority system, as it allows critical workloads to be scheduled even in resource-constrained environments.
