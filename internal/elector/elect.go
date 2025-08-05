package elector

import (
	"context"
	"log"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type LeaderElector struct {
	LeaseDuration    time.Duration
	RenewDeadline    time.Duration
	RetryPeriod      time.Duration
	Name             string
	Namespace        string
	Config           *rest.Config
	Host             string
	OnStartedLeading func(ctx context.Context)
	OnStoppedLeading func()
	OnNewLeader      func(identity string)
}

func (t *LeaderElector) Run(ctx *context.Context) {
	clientset, err := kubernetes.NewForConfig(t.Config)
	if err != nil {
		log.Fatalf("failed to get create config, err=%v", err)
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      t.Name,
			Namespace: t.Namespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: t.Host,
		},
	}
	go func() {
		leaderelection.RunOrDie(*ctx, leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: t.LeaseDuration,
			RenewDeadline: t.RenewDeadline,
			RetryPeriod:   t.RetryPeriod,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: t.OnStartedLeading,
				OnStoppedLeading: t.OnStoppedLeading,
				OnNewLeader:      t.OnNewLeader,
			},
			ReleaseOnCancel: true,
			Name:            "orange",
		})
	}()
}
