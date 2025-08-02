/*
Copyright 2025.

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
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ctlv1alpha1 "orangectl/api/v1alpha1"
)

var _ = Describe("OrangeCtl Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		resource := &ctlv1alpha1.OrangeCtl{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: "default",
			},
			Spec: ctlv1alpha1.OrangeCtlSpec{
				Namespace: "default",
				Router: ctlv1alpha1.RouterSpec{
					Name:   "router",
					Labels: map[string]string{},
					Image:  "docker.io/nagsbixy/gateway:latest",
					Port:   8000,
				},
				Shard: ctlv1alpha1.ShardSpec{
					Name:     "shard",
					Labels:   map[string]string{},
					Image:    "docker.io/nagsbixy/orange:latest",
					Count:    1,
					Replicas: 1,
					Port:     52001,
				},
			},
		}

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		orangectl := &ctlv1alpha1.OrangeCtl{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind OrangeCtl")
			err := k8sClient.Get(ctx, typeNamespacedName, orangectl)
			if err != nil && errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &ctlv1alpha1.OrangeCtl{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance OrangeCtl")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &OrangeCtlReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// should create router Deployment object
			deploy := appsv1.Deployment{}
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: resource.Spec.Router.Name}, &deploy)
			Expect(err).NotTo(HaveOccurred())

		})
	})
})
