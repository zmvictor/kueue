# WorkloadPriority System in Kueue

## 1. WorkloadPriorityClass Definition

```
┌─────────────────────────────────────────┐
│ WorkloadPriorityClass                   │
├─────────────────────────────────────────┤
│ metav1.TypeMeta                         │
│ metav1.ObjectMeta                       │
│                                         │
│ Value: int32                            │ ◄── Priority value used for scheduling
│ Description: string (optional)          │ ◄── Usage guidelines
└─────────────────────────────────────────┘
```

The `WorkloadPriorityClass` is a Kubernetes custom resource that defines a mapping from a priority class name to a priority value. It contains:
- `Value`: An integer representing the priority (higher values indicate higher priority)
- `Description`: An optional string providing guidelines on when to use this priority class

## 2. Workload Priority Reference Mechanism

```
┌─────────────────────────────────────────┐
│ Job/Workload                            │
├─────────────────────────────────────────┤
│ metadata:                               │
│   labels:                               │
│     kueue.x-k8s.io/priority-class: "high-priority"  │ ◄── References WorkloadPriorityClass by name
│ ...                                     │
└─────────────────────────────────────────┘
                      │
                      │ references
                      ▼
┌─────────────────────────────────────────┐
│ WorkloadPriorityClass                   │
│ name: "high-priority"                   │
├─────────────────────────────────────────┤
│ Value: 1000                             │
│ Description: "For critical workloads"   │
└─────────────────────────────────────────┘
```

Workloads reference priority classes through the `kueue.x-k8s.io/priority-class` label, which contains the name of the WorkloadPriorityClass resource.

## 3. Priority Resolution Flow

```
┌────────────────┐     ┌────────────────────────┐     ┌────────────────────────┐
│                │     │                        │     │                        │
│  Job Creation  │────►│  Workload Controller   │────►│  Workload Creation     │
│                │     │                        │     │                        │
└────────────────┘     └────────────────────────┘     └───────────┬────────────┘
                                                                   │
                                                                   │ Label extraction
                                                                   ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ WorkloadPriorityClassName(object client.Object) string {                           │
│   if workloadPriorityClassLabel := object.GetLabels()[constants.WorkloadPriorityClassLabel]; │
│      workloadPriorityClassLabel != "" {                                            │
│     return workloadPriorityClassLabel                                              │
│   }                                                                                │
│   return ""                                                                        │
│ }                                                                                  │
└────────────────────────────────────────────────────────────────────────────────────┘
                                                                   │
                                                                   │ Priority class name
                                                                   ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ GetPriorityFromWorkloadPriorityClass(ctx, client, workloadPriorityClass) {         │
│   wpc := &kueue.WorkloadPriorityClass{}                                            │
│   if err := client.Get(ctx, types.NamespacedName{Name: workloadPriorityClass}, wpc); │
│      err != nil {                                                                   │
│     return "", "", 0, err                                                           │
│   }                                                                                 │
│   return wpc.Name, constants.WorkloadPriorityClassSource, wpc.Value, nil           │
│ }                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────┘
                                                                   │
                                                                   │ Priority value
                                                                   ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ Workload                                                                           │
├────────────────────────────────────────────────────────────────────────────────────┤
│ Spec:                                                                              │
│   Priority: 1000  ◄── Priority value set from WorkloadPriorityClass                │
│   ...                                                                              │
└────────────────────────────────────────────────────────────────────────────────────┘
                                                                   │
                                                                   │ Priority retrieval
                                                                   ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│ Priority(w *kueue.Workload) int32 {                                                │
│   return ptr.Deref(w.Spec.Priority, constants.DefaultPriority)                     │
│ }                                                                                   │
└────────────────────────────────────────────────────────────────────────────────────┘
```

The priority resolution flow shows how a priority class reference in a job is transformed into a priority value in the workload, which is then used by the scheduler.

## 4. Priority Usage in Scheduling

### Classical Iterator (Non-Fair Sharing)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Scheduler - Classical Iterator                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ Ordering criteria:                                                          │
│ 1. Request under nominal quota before borrowing                             │
│ 2. Higher priority first (if PrioritySortingWithinCohort enabled)           │
│ 3. FIFO based on creation/eviction timestamp                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ // Less is the ordering criteria                                            │
│ func (e entryOrdering) Less(i, j int) bool {                                │
│   a := e.entries[i]                                                         │
│   b := e.entries[j]                                                         │
│                                                                             │
│   // 1. Request under nominal quota.                                        │
│   aBorrows := a.assignment.Borrows()                                        │
│   bBorrows := b.assignment.Borrows()                                        │
│   if aBorrows != bBorrows {                                                 │
│     return !aBorrows                                                        │
│   }                                                                         │
│                                                                             │
│   // 2. Higher priority first if not disabled.                              │
│   if features.Enabled(features.PrioritySortingWithinCohort) {               │
│     p1 := priority.Priority(a.Obj)                                          │
│     p2 := priority.Priority(b.Obj)                                          │
│     if p1 != p2 {                                                           │
│       return p1 > p2                                                        │
│     }                                                                       │
│   }                                                                         │
│                                                                             │
│   // 3. FIFO.                                                               │
│   aComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(a.Obj)  │
│   bComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(b.Obj)  │
│   return aComparisonTimestamp.Before(bComparisonTimestamp)                  │
│ }                                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Fair Sharing Iterator

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Scheduler - Fair Sharing Iterator                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│ Ordering criteria:                                                          │
│ 1. Dominant Resource Share (DRS)                                            │
│ 2. Higher priority first (if PrioritySortingWithinCohort enabled)           │
│ 3. FIFO based on creation/eviction timestamp                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ func (e *entryComparer) less(a, b *entry, parentCohort kueue.CohortReference) bool { │
│   aDrs := e.drsValues[drsKey{parentCohort: parentCohort, workloadKey: workload.Key(a.Obj)}] │
│   bDrs := e.drsValues[drsKey{parentCohort: parentCohort, workloadKey: workload.Key(b.Obj)}] │
│   // 1: DRF                                                                 │
│   if aDrs != bDrs {                                                         │
│     return aDrs < bDrs                                                      │
│   }                                                                         │
│                                                                             │
│   // 2: Priority                                                            │
│   if features.Enabled(features.PrioritySortingWithinCohort) {               │
│     p1 := priority.Priority(a.Obj)                                          │
│     p2 := priority.Priority(b.Obj)                                          │
│     if p1 != p2 {                                                           │
│       return p1 > p2                                                        │
│     }                                                                       │
│   }                                                                         │
│                                                                             │
│   // 3: FIFO                                                                │
│   aComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(a.Obj)  │
│   bComparisonTimestamp := e.workloadOrdering.GetQueueOrderTimestamp(b.Obj)  │
│   return aComparisonTimestamp.Before(bComparisonTimestamp)                  │
│ }                                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 5. Preemption Based on Priority

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ ClusterQueue Configuration                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ Preemption:                                                                 │
│   WithinClusterQueue: PreemptionPolicyLowerPriority                         │ ◄── Enables priority-based preemption
│   ReclaimWithinCohort: PreemptionPolicyAny                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Scheduler                                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. High priority workload cannot be scheduled due to resource constraints   │
│ 2. Scheduler identifies lower priority workloads that could be preempted    │
│ 3. Lower priority workloads are evicted to make room for higher priority    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 6. Complete WorkloadPriority Lifecycle

```
┌─────────────────────┐
│ Define Priority     │
│ Classes             │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Create Job with     │
│ Priority Class Label│
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Workload Controller │
│ Creates Workload    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Extract Priority    │
│ Class Name from     │
│ Workload Label      │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Resolve Priority    │
│ Value from          │
│ WorkloadPriorityClass│
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Set Priority Value  │
│ in Workload.Spec    │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Scheduler Uses      │
│ Priority for:       │
│ - Ordering          │
│ - Preemption        │
└─────────────────────┘
```
