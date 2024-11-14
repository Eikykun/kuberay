package expectations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRayClusterExpectationsHeadPod(t *testing.T) {
	ctx := context.TODO()
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	testPods := getTestPod()

	// Expect create head pod.
	exp.ExpectScalePod(rayClusterName, HeadGroup, namespace, testPods[0].Name, Create)
	// There is no head pod in Informer, return false.
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, HeadGroup, namespace), false)
	// Add a pod to the informer. This is used to simulate the informer syncing with the head pod in etcd.
	// In reality, it should be automatically done by the informer.
	err := fakeClient.Create(ctx, testPods[0])
	assert.NoError(t, err, "Fail to create head pod")
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, HeadGroup, namespace), true)
	// Expect delete head pod.
	exp.ExpectScalePod(rayClusterName, HeadGroup, namespace, testPods[0].Name, Delete)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, HeadGroup, namespace), false)
	// Delete head pod from the informer.
	err = fakeClient.Delete(ctx, testPods[0])
	assert.NoError(t, err, "Fail to delete head pod")
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, HeadGroup, namespace), true)
}

func TestRayClusterExpectationsWorkerGroupPods(t *testing.T) {
	ctx := context.TODO()
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	groupA := "test-group-a"
	groupB := "test-group-b"
	testPods := getTestPod()
	// Expect create one worker pod in group-a, two worker pods in group-b.
	exp.ExpectScalePod(rayClusterName, groupA, namespace, testPods[0].Name, Create)
	exp.ExpectScalePod(rayClusterName, groupB, namespace, testPods[1].Name, Create)
	exp.ExpectScalePod(rayClusterName, groupB, namespace, testPods[2].Name, Create)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupA, namespace), false)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupB, namespace), false)
	assert.NoError(t, fakeClient.Create(ctx, testPods[1]), "Fail to create worker pod2")
	// All pods within the same group are expected to meet.
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupB, namespace), false)
	assert.NoError(t, fakeClient.Create(ctx, testPods[2]), "Fail to create worker pod3")
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupB, namespace), true)
	// Different groups do not affect each other.
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupA, namespace), false)
	assert.NoError(t, fakeClient.Create(ctx, testPods[0]), "Fail to create worker pod1")
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupA, namespace), true)

	// Expect delete.
	exp.ExpectScalePod(rayClusterName, groupA, namespace, testPods[0].Name, Delete)
	exp.ExpectScalePod(rayClusterName, groupB, namespace, testPods[1].Name, Delete)
	exp.ExpectScalePod(rayClusterName, groupB, namespace, testPods[2].Name, Delete)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupA, namespace), false)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupB, namespace), false)
	assert.NoError(t, fakeClient.Delete(ctx, testPods[1]), "Fail to delete worker pod2")
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupB, namespace), false)
	assert.NoError(t, fakeClient.Delete(ctx, testPods[2]), "Fail to delete worker pod3")
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupB, namespace), true)
	// Different groups do not affect each other.
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupA, namespace), false)
	assert.NoError(t, fakeClient.Delete(ctx, testPods[0]), "Fail to delete worker pod1")
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, groupA, namespace), true)
}

func TestRayClusterExpectationsDeleteAll(t *testing.T) {
	ctx := context.TODO()
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	group := "test-group"
	testPods := getTestPod()
	exp.ExpectScalePod(rayClusterName, HeadGroup, namespace, testPods[0].Name, Create)
	exp.ExpectScalePod(rayClusterName, group, namespace, testPods[1].Name, Create)
	exp.ExpectScalePod(rayClusterName, group, namespace, testPods[2].Name, Delete)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, HeadGroup, namespace), false)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, group, namespace), false)
	// Delete all expectations
	exp.Delete(rayClusterName, namespace)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, HeadGroup, namespace), true)
	assert.Equal(t, exp.IsSatisfied(ctx, rayClusterName, group, namespace), true)
}

func getTestPod() []*corev1.Pod {
	return []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod1",
				Namespace: "default",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod2",
				Namespace: "default",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod3",
				Namespace: "default",
			},
		},
	}
}
