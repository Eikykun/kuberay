package expectations

import (
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

type Action string

const (
	Separator = "/"

	Delete Action = "Delete"
	Create Action = "Create"

	DefaultHeadGroup = ""
)

type RayClusterExpectationInterface interface {
	ExpectPodDeletions(namespace, podName string) error
	ExpectHeadCreations(rayClusterKey string, adds int) error
	ExpectHeadDeletions(rayClusterKey string, dels int) error
	ExpectWorkerDeletions(rayClusterKey, group string, dels int) error
	ExpectWorkerCreations(rayClusterKey, group string, adds int) error

	Observed(rayClusterKey, podName string, action Action)
	ObservedHead(rayClusterKey string, action Action)
	ObservedWorker(rayClusterKey, group string, action Action)

	DeleteExpectations(rayClusterKey string)
	DeleteHeadExpectations(rayClusterKey string)
	DeleteWorkerExpectations(rayClusterKey, group string)

	SatisfiedHeadExpectations(rayClusterKey string) bool
	SatisfiedWorkerExpectations(rayClusterKey, group string) bool
}

type RayClusterExpectation struct {
	mu         sync.RWMutex
	groupStore map[string]sets.Set[string]
	exp        ControllerExpectationsInterface
}

func (re *RayClusterExpectation) SatisfiedHeadExpectations(rayClusterKey string) bool {
	return re.exp.SatisfiedExpectations(rayClusterGroupKey(rayClusterKey, DefaultHeadGroup))
}

func (re *RayClusterExpectation) SatisfiedWorkerExpectations(rayClusterKey, group string) bool {
	return re.exp.SatisfiedExpectations(rayClusterGroupKey(rayClusterKey, group))
}

func (re *RayClusterExpectation) ExpectPodDeletions(namespace, podName string) error {
	vals := strings.Split(podName, utils.DashSymbol)
	// head   : {instance}-head-{Hash}
	// worker : {instance}-worker-{Group}-{Hash}
	group := DefaultHeadGroup
	if len(vals) > 2 {
		group = vals[2]
	} else if len(vals) < 2 {
		return fmt.Errorf("unexpected pod name: %s", podName)
	}
	rayClusterKey := namespace + "/" + vals[0]
	re.recordGroup(rayClusterKey, group)
	return re.exp.ExpectDeletions(rayClusterGroupKey(rayClusterKey, group), 1)
}

func (re *RayClusterExpectation) ExpectWorkerDeletions(rayClusterKey, group string, dels int) error {
	group = strings.ToLower(group)
	re.recordGroup(rayClusterKey, group)
	return re.exp.ExpectDeletions(rayClusterGroupKey(rayClusterKey, group), dels)
}

func (re *RayClusterExpectation) ExpectHeadDeletions(rayClusterKey string, dels int) error {
	re.recordGroup(rayClusterKey, DefaultHeadGroup)
	return re.exp.ExpectDeletions(rayClusterGroupKey(rayClusterKey, DefaultHeadGroup), dels)
}

func (re *RayClusterExpectation) ExpectWorkerCreations(rayClusterKey, group string, adds int) error {
	group = strings.ToLower(group)
	re.recordGroup(rayClusterKey, group)
	return re.exp.ExpectCreations(rayClusterGroupKey(rayClusterKey, group), adds)
}

func (re *RayClusterExpectation) ExpectHeadCreations(rayClusterKey string, adds int) error {
	re.recordGroup(rayClusterKey, DefaultHeadGroup)
	return re.exp.ExpectCreations(rayClusterGroupKey(rayClusterKey, DefaultHeadGroup), adds)
}

func (re *RayClusterExpectation) Observed(namespace, podName string, action Action) {
	vals := strings.Split(podName, utils.DashSymbol)
	// head   : {instance}-head-{Hash}
	// worker : {instance}-worker-{Group}-{Hash}
	group := DefaultHeadGroup
	if len(vals) > 2 {
		group = vals[2]
	} else if len(vals) < 2 {
		return
	}
	rayClusterKey := namespace + "/" + vals[0]

	if group == DefaultHeadGroup {
		re.ObservedHead(rayClusterKey, action)
	} else {
		re.ObservedWorker(rayClusterKey, group, action)
	}
}

func (re *RayClusterExpectation) ObservedHead(rayClusterKey string, action Action) {
	key := rayClusterGroupKey(rayClusterKey, DefaultHeadGroup)
	switch action {
	case Create:
		re.exp.CreationObserved(key)
	case Delete:
		re.exp.DeletionObserved(key)
	}
}

func (re *RayClusterExpectation) ObservedWorker(rayClusterKey, group string, action Action) {
	key := rayClusterGroupKey(rayClusterKey, group)
	switch action {
	case Create:
		re.exp.CreationObserved(key)
	case Delete:
		re.exp.DeletionObserved(key)
	}
}

func (re *RayClusterExpectation) DeleteExpectations(rayClusterKey string) {
	re.mu.Lock()
	defer re.mu.Unlock()
	groups, ok := re.groupStore[rayClusterKey]
	if !ok || groups == nil {
		return
	}
	for group := range groups {
		re.exp.DeleteExpectations(rayClusterGroupKey(rayClusterKey, group))
	}
	delete(re.groupStore, rayClusterKey)
}

func (re *RayClusterExpectation) DeleteHeadExpectations(rayClusterKey string) {
	re.mu.Lock()
	defer re.mu.Unlock()
	groups, ok := re.groupStore[rayClusterKey]
	if !ok || groups == nil || !groups.Has(DefaultHeadGroup) {
		return
	}
	groups.Delete(DefaultHeadGroup)
	re.exp.DeleteExpectations(rayClusterGroupKey(rayClusterKey, DefaultHeadGroup))
}

func (re *RayClusterExpectation) DeleteWorkerExpectations(rayClusterKey, group string) {
	re.mu.Lock()
	defer re.mu.Unlock()
	groups, ok := re.groupStore[rayClusterKey]
	if !ok || groups == nil || !groups.Has(group) {
		return
	}
	groups.Delete(group)
	re.exp.DeleteExpectations(rayClusterGroupKey(rayClusterKey, group))
}

func (re *RayClusterExpectation) recordGroup(rayClusterKey, group string) {
	re.mu.Lock()
	defer re.mu.Unlock()
	groups, ok := re.groupStore[rayClusterKey]
	if !ok || groups == nil {
		groups = sets.New[string](group)
		re.groupStore[rayClusterKey] = groups
	} else if !groups.Has(group) {
		groups.Insert(group)
	}
}

func rayClusterGroupKey(rayClusterKey, group string) (key string) {
	if group == DefaultHeadGroup {
		key = rayClusterKey + Separator + string(rayv1.HeadNode)
	} else {
		key = rayClusterKey + Separator + string(rayv1.WorkerNode) + Separator + group
	}
	return key
}

func NewRayClusterExpectation(name string) RayClusterExpectationInterface {
	return &RayClusterExpectation{
		groupStore: make(map[string]sets.Set[string]),
		exp:        NewControllerExpectations(name),
	}
}
