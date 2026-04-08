/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	namespaceclassv1alpha1 "github.com/yhxlele/namespaceclass/api/v1alpha1"
)

const (
	LabelClassName      = "namespaceclass.akuity.io/name"
	LastReconciled      = "namespaceclass.akuity.io/last-reconciled-name"
	ManagedByValue      = "namespaceclass.akuity.io"
	FieldManager        = "namespaceclass.akuity.io/controller"
	requeueMissingClass = 30 * time.Second
)

type NamespaceReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	RESTMapper      meta.RESTMapper
	DiscoveryClient discovery.DiscoveryInterface
	Recorder        events.EventRecorder
}

// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=namespaceclass.akuity.io,resources=namespaceclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups=*,resources=*,verbs=get;list;watch;create;update;patch;delete

func (r *NamespaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("namespace", req.Name)
	ctx = logf.IntoContext(ctx, logger)

	ns := &corev1.Namespace{}
	if err := r.Get(ctx, types.NamespacedName{Name: req.Name}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	className := strings.TrimSpace(ns.Labels[LabelClassName])
	last := strings.TrimSpace(ns.Annotations[LastReconciled])

	if className == "" && last == "" {
		return ctrl.Result{}, nil
	}

	if className == "" {
		_, apiResourceLists, discErr := r.DiscoveryClient.ServerGroupsAndResources()
		if discErr != nil {
			return ctrl.Result{}, fmt.Errorf("discovery: %w", discErr)
		}
		if err := r.deleteManagedObjectsForClass(ctx, ns.Name, last, apiResourceLists); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.clearLastReconciled(ctx, ns); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	nc := &namespaceclassv1alpha1.NamespaceClass{}
	if err := r.Get(ctx, types.NamespacedName{Name: className}, nc); err != nil {
		if apierrors.IsNotFound(err) {
			r.Recorder.Eventf(ns, nil, corev1.EventTypeWarning, "NamespaceClassMissing", "ReconcileNamespace",
				"NamespaceClass %q not found; skipping apply and delete", className)
			return ctrl.Result{RequeueAfter: requeueMissingClass}, nil
		}
		return ctrl.Result{}, err
	}

	desired, validateErr := r.buildDesiredObjects(ns.Name, className, nc)
	if validateErr != nil {
		r.Recorder.Eventf(ns, nil, corev1.EventTypeWarning, "InvalidNamespaceClass", "ReconcileNamespace",
			"Invalid NamespaceClass templates: %v", validateErr)
		return ctrl.Result{}, validateErr
	}

	_, apiResourceLists, discErr := r.DiscoveryClient.ServerGroupsAndResources()
	if discErr != nil {
		return ctrl.Result{}, fmt.Errorf("discovery: %w", discErr)
	}

	if last != "" && last != className {
		if err := r.deleteManagedObjectsForClass(ctx, ns.Name, last, apiResourceLists); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.deleteDrift(ctx, ns.Name, className, desired, apiResourceLists); err != nil {
		return ctrl.Result{}, err
	}

	ownerRef := metav1.OwnerReference{
		APIVersion:         "v1",
		Kind:               "Namespace",
		Name:               ns.Name,
		UID:                ns.UID,
		BlockOwnerDeletion: ptr.To(false),
		Controller:         ptr.To(false),
	}

	for _, d := range desired {
		obj := d.DeepCopy()
		if err := r.applyObject(ctx, ownerRef, obj); err != nil {
			return ctrl.Result{}, err
		}
	}

	if err := r.setLastReconciled(ctx, ns, className); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *NamespaceReconciler) listManagedObjectsForClass(
	ctx context.Context,
	namespace, className string,
	apiResourceLists []*metav1.APIResourceList,
) ([]unstructured.Unstructured, error) {
	sel, err := labels.Parse(
		fmt.Sprintf("%s=%s,%s=%s", LabelClassName, className, "app.kubernetes.io/managed-by", ManagedByValue),
	)
	if err != nil {
		return nil, err
	}

	lists := apiResourceLists
	if lists == nil {
		_, l, discErr := r.DiscoveryClient.ServerGroupsAndResources()
		if discErr != nil {
			return nil, fmt.Errorf("discovery: %w", discErr)
		}
		lists = l
	}

	var out []unstructured.Unstructured
	for _, rl := range lists {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			continue
		}
		for _, res := range rl.APIResources {
			if !res.Namespaced || !containsVerb(res.Verbs, "list") || strings.Contains(res.Name, "/") {
				continue
			}
			gvk := gv.WithKind(res.Kind)
			uList := &unstructured.UnstructuredList{}
			uList.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   gvk.Group,
				Version: gvk.Version,
				Kind:    res.Kind + "List",
			})
			if listErr := r.List(ctx, uList,
				client.InNamespace(namespace),
				client.MatchingLabelsSelector{Selector: sel},
			); listErr != nil {
				continue
			}
			out = append(out, uList.Items...)
		}
	}
	return out, nil
}

func containsVerb(verbs []string, v string) bool {
	return slices.Contains(verbs, v)
}

func (r *NamespaceReconciler) deleteManagedObjectsForClass(
	ctx context.Context,
	namespace, className string,
	apiResourceLists []*metav1.APIResourceList,
) error {
	logger := logf.FromContext(ctx)
	if className == "" {
		return nil
	}
	objs, err := r.listManagedObjectsForClass(ctx, namespace, className, apiResourceLists)
	if err != nil {
		return err
	}
	for i := range objs {
		o := &objs[i]
		if err := r.Delete(ctx, o); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to delete managed object", "gvk", o.GroupVersionKind().String(), "name", o.GetName())
			return fmt.Errorf("delete %s %s/%s: %w", o.GroupVersionKind().String(), o.GetNamespace(), o.GetName(), err)
		}
		logger.V(1).Info("Deleted managed object", "gvk", o.GroupVersionKind().String(), "name", o.GetName())
	}
	return nil
}

func (r *NamespaceReconciler) clearLastReconciled(ctx context.Context, ns *corev1.Namespace) error {
	logger := logf.FromContext(ctx)
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &corev1.Namespace{}
		if err := r.Get(ctx, types.NamespacedName{Name: ns.Name}, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		ann := latest.GetAnnotations()
		if ann == nil {
			return nil
		}
		delete(ann, LastReconciled)
		if len(ann) == 0 {
			latest.SetAnnotations(nil)
		} else {
			latest.SetAnnotations(ann)
		}
		if err := r.Patch(ctx, latest, client.MergeFrom(base)); err != nil {
			return err
		}
		logger.V(1).Info("Cleared last-reconciled annotation", "namespace", ns.Name)
		return nil
	})
}

func (r *NamespaceReconciler) setLastReconciled(ctx context.Context, ns *corev1.Namespace, className string) error {
	logger := logf.FromContext(ctx)
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &corev1.Namespace{}
		if err := r.Get(ctx, types.NamespacedName{Name: ns.Name}, latest); err != nil {
			return err
		}
		base := latest.DeepCopy()
		ann := latest.GetAnnotations()
		if ann == nil {
			ann = map[string]string{}
		}
		ann[LastReconciled] = className
		latest.SetAnnotations(ann)
		if err := r.Patch(ctx, latest, client.MergeFrom(base)); err != nil {
			return err
		}
		logger.V(1).Info("Set last-reconciled annotation", "namespace", ns.Name, "class", className)
		return nil
	})
}

func (r *NamespaceReconciler) buildDesiredObjects(
	namespaceName, className string,
	nc *namespaceclassv1alpha1.NamespaceClass,
) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	for i, raw := range nc.Spec.Resources {
		u, err := decodeRawExtension(raw)
		if err != nil {
			return nil, fmt.Errorf("resources[%d]: %w", i, err)
		}
		gvk := u.GroupVersionKind()
		if gvk.Version == "" || gvk.Kind == "" {
			return nil, fmt.Errorf("resources[%d]: apiVersion and kind are required", i)
		}
		if u.GetName() == "" {
			return nil, fmt.Errorf("resources[%d]: metadata.name is required", i)
		}
		mapping, err := r.RESTMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("resources[%d]: unknown apiVersion/kind %s: %w", i, gvk.String(), err)
		}
		if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
			return nil, fmt.Errorf("resources[%d]: kind %s is cluster-scoped", i, gvk.Kind)
		}
		u.SetNamespace(namespaceName)
		lbls := u.GetLabels()
		if lbls == nil {
			lbls = map[string]string{}
		}
		lbls[LabelClassName] = className
		lbls["app.kubernetes.io/managed-by"] = ManagedByValue
		u.SetLabels(lbls)
		u.SetOwnerReferences(nil)
		out = append(out, u)
	}
	return out, nil
}

func decodeRawExtension(raw runtime.RawExtension) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	if len(raw.Raw) == 0 {
		return nil, fmt.Errorf("empty resource blob")
	}
	if err := json.Unmarshal(raw.Raw, &u.Object); err != nil {
		return nil, err
	}
	return u, nil
}

func mappingKey(u *unstructured.Unstructured) string {
	gvk := u.GroupVersionKind()
	return fmt.Sprintf("%s/%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind, u.GetName())
}

func (r *NamespaceReconciler) deleteDrift(
	ctx context.Context,
	namespace, className string,
	desired []*unstructured.Unstructured,
	apiResourceLists []*metav1.APIResourceList,
) error {
	logger := logf.FromContext(ctx)
	shouldExist := make(map[string]struct{}, len(desired))
	for _, u := range desired {
		shouldExist[mappingKey(u)] = struct{}{}
	}
	existing, err := r.listManagedObjectsForClass(ctx, namespace, className, apiResourceLists)
	if err != nil {
		return err
	}
	for i := range existing {
		e := &existing[i]
		if _, ok := shouldExist[mappingKey(e)]; ok {
			continue
		}
		if err := r.Delete(ctx, e); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to delete drift object", "gvk", e.GroupVersionKind().String(), "name", e.GetName())
			return fmt.Errorf("delete %s %s/%s: %w", e.GroupVersionKind().String(), e.GetNamespace(), e.GetName(), err)
		}
		logger.V(1).Info("Deleted drift object", "gvk", e.GroupVersionKind().String(), "name", e.GetName())
	}
	return nil
}

func (r *NamespaceReconciler) applyObject(
	ctx context.Context,
	owner metav1.OwnerReference,
	u *unstructured.Unstructured,
) error {
	logger := logf.FromContext(ctx)
	u = u.DeepCopy()
	u.SetOwnerReferences([]metav1.OwnerReference{owner})
	u.SetResourceVersion("")
	applyOpts := []client.ApplyOption{
		client.ForceOwnership,
		client.FieldOwner(FieldManager),
	}
	ac := client.ApplyConfigurationFromUnstructured(u)
	if err := r.Apply(ctx, ac, applyOpts...); err != nil {
		logger.Error(err, "Failed to apply object", "gvk", u.GroupVersionKind().String(), "name", u.GetName())
		return fmt.Errorf("apply %s %s/%s: %w", u.GroupVersionKind().String(), u.GetNamespace(), u.GetName(), err)
	}
	logger.V(1).Info("Applied object", "gvk", u.GroupVersionKind().String(), "name", u.GetName())
	return nil
}

func (r *NamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}, builder.WithPredicates(predicate.NewPredicateFuncs(namespaceNeedsReconcile))).
		Watches(
			&namespaceclassv1alpha1.NamespaceClass{},
			handler.EnqueueRequestsFromMapFunc(r.findNamespacesForNamespaceClass),
		).
		Named("namespace").
		Complete(r)
}

func namespaceNeedsReconcile(obj client.Object) bool {
	ns, ok := obj.(*corev1.Namespace)
	if !ok {
		return false
	}
	if ns.Labels != nil && strings.TrimSpace(ns.Labels[LabelClassName]) != "" {
		return true
	}
	if ns.Annotations != nil && strings.TrimSpace(ns.Annotations[LastReconciled]) != "" {
		return true
	}
	return false
}

func (r *NamespaceReconciler) findNamespacesForNamespaceClass(ctx context.Context, obj client.Object) []reconcile.Request {
	nc, ok := obj.(*namespaceclassv1alpha1.NamespaceClass)
	if !ok {
		return nil
	}
	list := &corev1.NamespaceList{}
	sel := client.MatchingLabels{LabelClassName: nc.GetName()}
	if err := r.List(ctx, list, sel); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list namespaces for NamespaceClass", "namespaceClass", nc.GetName())
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for _, ns := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: ns.Name}})
	}
	return reqs
}
