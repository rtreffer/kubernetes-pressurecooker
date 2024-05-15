package pressurecooker

import (
	"math"
	"sort"
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
)

type PodCandidateSet []PodCandidate

func (s PodCandidateSet) Len() int {
	return len(s)
}

func (s PodCandidateSet) Less(i, j int) bool {
	return s[i].Score < s[j].Score
}

func (s PodCandidateSet) Swap(i, j int) {
	x := s[i]
	s[i] = s[j]
	s[j] = x
}

type PodCandidate struct {
	Pod   *v1.Pod
	Score int
}

func PodCandidateSetFromPodList(l *v1.PodList) PodCandidateSet {
	s := make(PodCandidateSet, len(l.Items))

	for i := range l.Items {
		s[i] = PodCandidate{
			Pod:   &l.Items[i],
			Score: 0,
		}
	}

	return s
}

func (s PodCandidateSet) scoreByQOSClass() {
	for i := range s {
		switch s[i].Pod.Status.QOSClass {
		case v1.PodQOSBestEffort:
			s[i].Score += 100
		case v1.PodQOSBurstable:
			s[i].Score += 100
		}
	}
}

// scoreByAge scores Pods by their age, respecting a minimum age.
// This function assumes that a pod that has been running for a long time is less likely to be a bad neighbor. It is thus a "better"
// candidate for eviction.
func (s PodCandidateSet) scoreByAge(minPodAge time.Duration) {
	// The scale of the score (currently ~20-40) is not very important as it is the only function scoring the pods in a linear fashion.
	// TODO: this needs to be revisited if multiple dimensions are taken into account (e.g. actual resource requests or usage).
	now := time.Now()
	for i, pod := range s {
		if pod.Pod.Status.StartTime == nil {
			s[i].Score -= 10000
			continue
		}
		delta := now.Sub(pod.Pod.Status.StartTime.Time)
		if delta < minPodAge {
			s[i].Score -= 10000
			continue
		}
		age := int64(delta / time.Second)
		if age < 1 {
			age = 1
		}
		// some values for age (as duration) and score:
		// 1s: 20
		// 1m: 24
		// 5m: 26
		// 1h: 28
		// 1d: 32
		// 7d: 34
		// 1y: 39
		s[i].Score += int(math.Floor(math.Log1p(float64(age))))
	}
}

func (s PodCandidateSet) scoreByOwnerType() {
	for i := range s {
		// do not evict Pods without owner; these will probably not be re-scheduled if evicted
		if len(s[i].Pod.OwnerReferences) == 0 {
			s[i].Score -= 1000
		}

		for j := range s[i].Pod.OwnerReferences {
			o := &s[i].Pod.OwnerReferences[j]

			switch o.Kind {
			case "ReplicaSet":
				s[i].Score += 100
			case "StatefulSet":
				s[i].Score -= 10000
			case "DaemonSet":
				s[i].Score -= 10000
			}
		}
	}
}

func (s PodCandidateSet) scoreByCriticality() {
	for i := range s {
		if s[i].Pod.Namespace == "kube-system" {
			s[i].Score -= 10000
		}

		switch s[i].Pod.Spec.PriorityClassName {
		case "system-cluster-critical":
			s[i].Score -= 10000
		case "system-node-critical":
			s[i].Score -= 10000
		}

		if _, ok := s[i].Pod.Annotations["scheduler.alpha.kubernetes.io/critical-pod"]; ok {
			s[i].Score -= 10000
		}
	}
}

// SelectPodForEviction selects a Pod for eviction.
// The selected Pod is the pod most likely to be "safe to evict".
// Safe to evict means:
// - The pod is not critical, unowned, a job or part of a daemonset (or any other case were we would either reschedule on this node or loose work/data)
// - The pod is unlikely to be a bad neighbor (estimated by time on the node)
// The idea of moving "good" pods is that we can with high confidence say that they will not cause any issues on the next node they are scheduled on.
// This is critical since we don't want to play a game of "whack-a-mole" where we evict a pod from one node only to have it cause issues on the next node.
// Evicting the oldest pods also protects against the case where a pod gets eviceted and rescheduled multiple times in a short period of time.
// The age of pods of the same deployment often correlates with the age of the deployment itself. However given HPAs and cluster autoscalers there is
// some variance in the age of pods in a deployment that protects a deployment from becoming a target on too manz nodes at the same time.
//
// Assumptions:
// - The younger a pod is the more likely it is to be a bad neighbor.
// - The older a pod is the more likely it is to be a good citizen.
// - The likelyhood of an overload is not purely a function of time (true if HPA and cluster autoscaler are used).
//
// The downside of this approach is that we might evict more pods as we isolate the "bad" pods. It might also take longer to get a node back to a healthy state.
//
// A common example would be a spark job that is running on a node. The spark job is using all the resources on the node if possible.
// Coupled with e.g. a low overload on other pods it might act as a bad neighbor, causing CPU issues for the other pods.
// This job should however not be evicted as similar issues would occur on the next node it is scheduled on. We also want the job to finish sometime.
// We are thus moving the "good" pods to other nodes until this node is healthy again, or at least within an acceptable level of overload.
func (s PodCandidateSet) SelectPodForEviction(minPodAge time.Duration) *v1.Pod {
	s.scoreByAge(minPodAge)
	s.scoreByQOSClass()
	s.scoreByOwnerType()
	s.scoreByCriticality()

	sort.Stable(sort.Reverse(s))

	for i := range s {
		glog.Infof("eviction candidate: %s/%s (score of %d)", s[i].Pod.Namespace, s[i].Pod.Name, s[i].Score)
	}

	for i := range s {
		if s[i].Score < 0 {
			continue
		}

		glog.Infof("selected candidate: %s/%s (score of %d)", s[i].Pod.Namespace, s[i].Pod.Name, s[i].Score)
		return s[i].Pod
	}

	return nil
}
