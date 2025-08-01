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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// OrangeCtlSpec defines the desired state of OrangeCtl
// OrangeCtlSpec defines the desired state of OrangeCtl
type OrangeCtlSpec struct {
	// Namespace in which all components (router, shards) will be deployed
	Namespace string `json:"namespace"`

	// Router defines the configuration for the proxy/router component
	Router RouterSpec `json:"router"`

	// Shard defines the configuration for the shard StatefulSets
	Shard ShardSpec `json:"shard"`
}

// RouterSpec defines the configuration for the router/proxy component
type RouterSpec struct {
	// Name of the router deployment/service
	Name string `json:"name"`

	// Labels to apply to the router deployment and service
	Labels map[string]string `json:"labels,omitempty"`

	// Container image for the router
	Image string `json:"image"`

	// Port exposed by the router container
	Port int32 `json:"port"`

	// Optional environment variables or config parameters
	Config map[string]string `json:"config,omitempty"`
}

// ShardSpec defines the configuration for the shards (StatefulSets)
type ShardSpec struct {
	// Base name prefix for shard StatefulSets (e.g., "shard" -> "shard-0", "shard-1", etc.)
	Name string `json:"name"`

	// Labels to apply to each shard StatefulSet and its pods
	Labels map[string]string `json:"labels,omitempty"`

	// Container image for each shard
	Image string `json:"image"`

	// Number of shards to deploy (each as a StatefulSet)
	Count int `json:"count"`

	// Number of replicas per shard StatefulSet
	Replicas int32 `json:"replicas"`

	// Port exposed by each shard container
	Port int32 `json:"port"`

	// Optional environment variables or config parameters
	Config map[string]string `json:"config,omitempty"`
}

// OrangeCtlStatus defines the observed state of OrangeCtl.
type OrangeCtlStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// OrangeCtl is the Schema for the orangectls API
type OrangeCtl struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of OrangeCtl
	// +required
	Spec OrangeCtlSpec `json:"spec"`

	// status defines the observed state of OrangeCtl
	// +optional
	Status OrangeCtlStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// OrangeCtlList contains a list of OrangeCtl
type OrangeCtlList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []OrangeCtl `json:"items"`
}

func init() {
	SchemeBuilder.Register(&OrangeCtl{}, &OrangeCtlList{})
}
