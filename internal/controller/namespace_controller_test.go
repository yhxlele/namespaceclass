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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientrest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	namespaceclassv1alpha1 "github.com/yhxlele/namespaceclass/api/v1alpha1"
)

var _ = Describe("Namespace Controller", func() {
	Context("When reconciling a labeled Namespace with a NamespaceClass", func() {
		const (
			className = "test-class"
			nsName    = "test-ns-reconcile"
			cmName    = "nc-managed-cm"
		)

		ctx := context.Background()
		rawCM := `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"` + cmName + `"},"data":{"hello":"world"}}`

		var (
			ns *corev1.Namespace
			nc *namespaceclassv1alpha1.NamespaceClass
		)

		BeforeEach(func() {
			nc = &namespaceclassv1alpha1.NamespaceClass{
				ObjectMeta: metav1.ObjectMeta{Name: className},
				Spec: namespaceclassv1alpha1.NamespaceClassSpec{
					Resources: []runtime.RawExtension{
						{Raw: []byte(rawCM)},
					},
				},
			}
			Expect(k8sClient.Create(ctx, nc)).To(Succeed())

			ns = &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: nsName,
					Labels: map[string]string{
						LabelClassName: className,
					},
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		})

		AfterEach(func() {
			_ = k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}})
			_ = k8sClient.Delete(ctx, &namespaceclassv1alpha1.NamespaceClass{ObjectMeta: metav1.ObjectMeta{Name: className}})
		})

		It("should apply ConfigMap from NamespaceClass via server-side apply", func() {
			httpClient, err := clientrest.HTTPClientFor(cfg)
			Expect(err).NotTo(HaveOccurred())
			rm, err := apiutil.NewDynamicRESTMapper(cfg, httpClient)
			Expect(err).NotTo(HaveOccurred())
			cs, err := kubernetes.NewForConfig(cfg)
			Expect(err).NotTo(HaveOccurred())

			reconciler := &NamespaceReconciler{
				Client:          k8sClient,
				Scheme:          k8sClient.Scheme(),
				RESTMapper:      rm,
				DiscoveryClient: cs.Discovery(),
				Recorder:        events.NewFakeRecorder(1024),
			}

			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: nsName},
			})
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: nsName, Name: cmName}, cm)).To(Succeed())
			Expect(cm.Data["hello"]).To(Equal("world"))
			Expect(cm.Labels[LabelClassName]).To(Equal(className))
			Expect(cm.Labels["app.kubernetes.io/managed-by"]).To(Equal(ManagedByValue))
		})
	})

	Context("When NamespaceClass is missing", func() {
		const nsName = "test-ns-missing-class"

		ctx := context.Background()

		It("should requeue without failing", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: nsName,
					Labels: map[string]string{
						LabelClassName: "nonexistent-class-xyz",
					},
				},
			}
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName}})
			}()

			httpClient, err := clientrest.HTTPClientFor(cfg)
			Expect(err).NotTo(HaveOccurred())
			rm, err := apiutil.NewDynamicRESTMapper(cfg, httpClient)
			Expect(err).NotTo(HaveOccurred())
			cs, err := kubernetes.NewForConfig(cfg)
			Expect(err).NotTo(HaveOccurred())

			reconciler := &NamespaceReconciler{
				Client:          k8sClient,
				Scheme:          k8sClient.Scheme(),
				RESTMapper:      rm,
				DiscoveryClient: cs.Discovery(),
				Recorder:        events.NewFakeRecorder(1024),
			}

			res, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: nsName},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(Equal(requeueMissingClass))

			phantom := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: nsName, Name: "should-not-exist"}, phantom)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})
	})
})
