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
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ctlv1alpha1 "orangectl/api/v1alpha1"
)

// OrangeCtlReconciler reconciles a OrangeCtl object
type OrangeCtlReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ctl.orangectl.orange.db,resources=orangectls,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ctl.orangectl.orange.db,resources=orangectls/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ctl.orangectl.orange.db,resources=orangectls/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the OrangeCtl object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *OrangeCtlReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var orangeCtl ctlv1alpha1.OrangeCtl
	if err := r.Get(ctx, req.NamespacedName, &orangeCtl); err != nil {
		if apierrors.IsNotFound(err) {
			log := logf.FromContext(ctx)
			log.Info("OrangeCtl resource not found (likely deleted), skipping")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get OrangeCtl resource")
		return ctrl.Result{}, err
	}

	// Reconcile Shard StatefulSets
	cfgMap, err := r.reconcileShards(ctx, &orangeCtl)
	if err != nil {
		log.Error(err, "Failed to reconcile shards")
		return ctrl.Result{}, err
	}

	// Reconcile Router Deployment and Service
	if err := r.reconcileRouter(ctx, &orangeCtl, cfgMap); err != nil {
		log.Error(err, "Failed to reconcile router")
		return ctrl.Result{}, err
	}

	log.Info("Successfully reconciled OrangeCtl")
	return ctrl.Result{}, nil
}

func (r *OrangeCtlReconciler) reconcileRouter(ctx context.Context, orangeCtl *ctlv1alpha1.OrangeCtl, cfgMap *corev1.ConfigMap) error {
	routerSpec := orangeCtl.Spec.Router
	namespace := orangeCtl.Spec.Namespace

	labels := routerSpec.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	labels["app"] = routerSpec.Name
	labels["orangectl"] = orangeCtl.Name

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      routerSpec.Name,
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		deploy.Labels = labels
		deploy.Spec = appsv1.DeploymentSpec{
			Replicas: pointer.Int32(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  routerSpec.Name,
							Image: routerSpec.Image,
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: routerSpec.Port,
								},
							},
							VolumeMounts: []corev1.VolumeMount{{
								Name:      "cfg",
								MountPath: "/app/config",
							}},
						},
					},
					Volumes: []corev1.Volume{{
						Name: "cfg",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: cfgMap.Name,
								},
							},
						},
					}},
				},
			},
		}
		return controllerutil.SetControllerReference(orangeCtl, deploy, r.Scheme)
	})

	if err != nil {
		return fmt.Errorf("failed to create/update router Deployment: %w", err)
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      routerSpec.Name,
			Namespace: namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = labels
		service.Spec = corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Port:       routerSpec.Port,
					TargetPort: intstr.FromInt(int(routerSpec.Port)),
				},
			},
		}
		return controllerutil.SetControllerReference(orangeCtl, service, r.Scheme)
	})

	if err != nil {
		return fmt.Errorf("failed to create/update router Service: %w", err)
	}

	return nil
}

func (r *OrangeCtlReconciler) reconcileShards(ctx context.Context, orangeCtl *ctlv1alpha1.OrangeCtl) (*corev1.ConfigMap, error) {
	shardSpec := orangeCtl.Spec.Shard
	namespace := orangeCtl.Spec.Namespace

	var addresses []string

	for i := 0; i < shardSpec.Count; i++ {
		shardName := fmt.Sprintf("%s-%d", shardSpec.Name, i)

		ss := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      shardName,
				Namespace: namespace,
			},
		}

		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ss, func() error {
			// Set labels, adding shard and orangeCtl labels
			if ss.Labels == nil {
				ss.Labels = map[string]string{}
			}
			for k, v := range shardSpec.Labels {
				ss.Labels[k] = v
			}
			ss.Labels["orangectl"] = orangeCtl.Name
			ss.Labels["shard"] = shardName

			// StatefulSet spec
			ss.Spec.ServiceName = shardName
			ss.Spec.Replicas = pointer.Int32(shardSpec.Replicas)
			ss.Spec.Selector = &metav1.LabelSelector{
				MatchLabels: ss.Labels,
			}
			ss.Spec.Template = corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ss.Labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "shard",
							Image: shardSpec.Image,
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: shardSpec.Port,
								},
							},
							Env: toEnvVars(shardSpec.Config),
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "data",
									MountPath: "/app/data",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			}

			return controllerutil.SetControllerReference(orangeCtl, ss, r.Scheme)
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create or update StatefulSet %s: %w", shardName, err)
		}

		// existingSvc := &corev1.Service{}
		// svcKey := types.NamespacedName{Name: shardName, Namespace: namespace}
		// err = r.Client.Get(ctx, svcKey, existingSvc)

		// if err == nil && len(existingSvc.Spec.Selector) == 0 {
		// 	// Delete the service if selector is missing (immutable field cannot be updated)
		// 	if delErr := r.Client.Delete(ctx, existingSvc); delErr != nil {
		// 		return nil, fmt.Errorf("failed to delete service %s for selector fix: %w", shardName, delErr)
		// 	}
		// 	// Optionally wait a little or re-fetch if your controller needs to avoid race
		// }

		service := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      shardName,
				Namespace: namespace,
			},
		}
		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
			service.Spec = corev1.ServiceSpec{
				ClusterIP: "None",
				Selector: map[string]string{
					"shard":     shardName, // should
					"orangectl": orangeCtl.Name,
				},
				Ports: []corev1.ServicePort{
					{
						Port:       shardSpec.Port,
						TargetPort: intstr.FromInt(int(shardSpec.Port)),
					},
				},
			}
			return controllerutil.SetControllerReference(orangeCtl, service, r.Scheme)
		})

		if err != nil {
			return nil, fmt.Errorf("failed to create or update StatefulSet service %s: %w", shardName, err)
		}

		for j := 0; j < int(shardSpec.Replicas); j++ {
			addresses = append(addresses, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local", shardName, j, shardName, namespace))
		}

	}

	cfgData, _ := json.Marshal(addresses)

	cfgMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shardSpec.Name + "-proxy-config",
			Namespace: namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cfgMap, func() error {
		if err := ctrl.SetControllerReference(orangeCtl, cfgMap, r.Scheme); err != nil {
			return err
		}
		cfgMap.Data = map[string]string{
			"SERVER_ADDRESS": string(cfgData),
		}
		return nil
	})

	return cfgMap, err
}

func toEnvVars(config map[string]string) []corev1.EnvVar {
	var envs []corev1.EnvVar
	for k, v := range config {
		envs = append(envs, corev1.EnvVar{
			Name:  k,
			Value: v,
		})
	}
	return envs
}

// SetupWithManager sets up the controller with the Manager.
func (r *OrangeCtlReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ctlv1alpha1.OrangeCtl{}).
		Named("orangectl").
		Complete(r)
}
