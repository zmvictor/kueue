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

Priority plays a crucial role in preemption decisions. When a high-priority workload cannot be scheduled due to resource constraints, the scheduler may decide to preempt (evict) lower-priority workloads to make room.

#### 5.2.1 Preemption Contexts

Kueue supports different preemption contexts:

1. **Within ClusterQueue Preemption**: When a high-priority workload cannot be scheduled within its ClusterQueue, Kueue can preempt lower-priority workloads in the same ClusterQueue.

2. **Cohort Reclamation**: When a ClusterQueue has loaned its quota to other ClusterQueues in the cohort, a workload can reclaim this quota by preempting workloads in other ClusterQueues.

3. **Borrowing Within Cohort**: A workload can simultaneously borrow quota from the cohort while preempting lower-priority workloads.

4. **Fair Sharing Preemption**: In fair sharing mode, preemption can occur to maintain a fair distribution of resources among ClusterQueues.

#### 5.2.2 Preemption Policies

The preemption policy is configured in the ClusterQueue's specification:

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

#### 5.2.3 Preemption Process Flow

When a workload requires preemption, the scheduler follows this process:

1. **Identify Candidates**: Based on the preemption policy, the scheduler identifies potential candidates for preemption. For example, with `LowerPriority` policy, only workloads with lower priority than the incoming workload are considered.

2. **Order Candidates**: Candidates are ordered with specific criteria:
   - Already evicted workloads first
   - Workloads from other ClusterQueues in the cohort before ones in the same ClusterQueue
   - Workloads with lower priority first
   - More recently admitted workloads first

3. **Find Minimal Set**: The scheduler tries to find the minimal set of workloads to preempt:
   - Simulates removing candidates one by one, starting with the lowest priority
   - Once the incoming workload fits, it stops removing candidates
   - Then tries to add back candidates in reverse order, as long as the incoming workload still fits

4. **Execute Preemption**: The selected workloads are marked for eviction with appropriate conditions and messages.

5. **Schedule Workload**: Once the preemption is complete, the scheduler attempts to schedule the higher-priority workload with the newly available resources.

This is reflected in the scheduler's code where it reserves resources for higher priority workloads:

```go
// we use resourcesToReserve to block capacity up to either the nominal capacity,
// or the borrowing limit when borrowing, so that a lower priority workload cannot
// admit before us.
cq.AddUsage(resourcesToReserve(e, cq))
```

#### 5.2.4 Preemption Safeguards

Kueue includes safeguards to prevent preemption cycles, where preempted workloads would immediately preempt their preemptor when they are requeued:

- When using `borrowWithinCohort` with a `maxPriorityThreshold`, workloads with priority above the threshold cannot be preempted, preventing high-priority workloads from being preempted by slightly higher priority workloads.
- The ordering of candidates ensures that workloads from other ClusterQueues are considered first, reducing the chance of preemption within the same queue.
- The minimal preemption algorithm tries to minimize disruption by finding the smallest set of workloads to preempt.

#### 5.2.5 Example: Preemption in Action

Consider a scenario where a high-priority workload (priority 100) needs to be scheduled and there are no available resources. The scheduler examines existing workloads:

```
Workload A: priority -10, using 2 CPUs
Workload B: priority 0, using 2 CPUs  
Workload C: priority 50, using 2 CPUs
```

If the new workload needs 4 CPUs and the preemption policy is `LowerPriority`, the scheduler would preempt workloads A and B to make room, as both have lower priority than the incoming workload.

The status of preempted workloads includes a `Preempted` condition with details about the preemptor:

```yaml
status:
  conditions:
  - lastTransitionTime: "2023-04-25T10:15:00Z"
    message: "Preempted to accommodate a workload (UID: abc-123, JobUID: job-456) due to prioritization in the ClusterQueue"
    reason: PreemptedByHigherPriorityWorkload
    status: "True"
    type: Preempted
```

When the preempted workloads are requeued, they will be considered for scheduling again based on their priority, but they won't be able to preempt the workload that preempted them (due to their lower priority).

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
