package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/nagarajRPoojari/orange/internal/elector"
	"github.com/nagarajRPoojari/orange/internal/utils"
	grpcserver "github.com/nagarajRPoojari/orange/net/server"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (

	// build modes
	__BUILD_MODE__ = "__BUILD_MODE__"
	__DEV_MODE__   = "__DEV_MODE__"
	__PROD_MODE__  = "__PROD_MODE__"

	// only when running sharded cluster in dev mode, i.e outside k8s
	// it uniquely identifies process/pod. And will be built in runtime
	// for other cases, i.e prod-sharded mode
	__HOST_ID__ = "__HOST_ID__"

	// expected only when running sharded cluster in prod
	__K8S_LEASE_NAMESAPCE__ = "__K8S_LEASE_NAMESAPCE__"
	__K8S_LEASE_NAME__      = "__K8S_LEASE_NAME__"
	__K8S_POD_NAME__        = "__K8S_POD_NAME__"
	__K8S_SERVICE_NAME__    = "__K8S_SERVICE_NAME__"

	// mode values
	__SHARDED__    = "sharded"
	__STANDALONE__ = "standalone"
	__DEV__        = "dev"
	__PROD__       = "prod"
)

var grpcSeverInstance *grpcserver.Server

func Start(addr string, port int64) {
	runSidecar()
	grpcSeverInstance = grpcserver.NewServer(addr, port)
	go grpcSeverInstance.Run()
}

func Stop() {
	if grpcSeverInstance != nil {
		grpcSeverInstance.Stop()
		log.Infof("server stopped successfully.")
	} else {
		log.Warnf("Attempted to stop server, but it was not running.")
	}
}

func runSidecar() {
	value := utils.GetEnv(__BUILD_MODE__, __DEV__)

	switch value {
	case __DEV__:
		runInDevMode()
	case __PROD__:
		runInProdMode()
	}
}

func runInDevMode() {
	value := utils.GetEnv(__DEV_MODE__, __STANDALONE__)

	switch value {
	case __SHARDED__:
		log.Infof("(Check dev mode): Running in `sharded` mode!")
		ctx := context.Background()

		config, err := clientcmd.BuildConfigFromFlags("", filepath.Join(os.Getenv("HOME"), ".kube", "config"))
		if err != nil {
			log.Fatalf("failed to extract kube config, err=%v", err)
		}

		id := utils.GetEnv(__HOST_ID__, uuid.NewString())
		lockNamespace := utils.GetEnv(__K8S_LEASE_NAMESAPCE__, "default")
		lockName := utils.GetEnv(__K8S_LEASE_NAME__, "orange-leader-election-lock")

		elector := &elector.LeaderElector{
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
			Name:          lockName,
			Namespace:     lockNamespace,
			Host:          id,
			Config:        config,
			OnStartedLeading: func(ctx context.Context) {
				log.Infof("I become leader")
			},
			OnStoppedLeading: func() {
				log.Infof("stpping down as a leader")
			},
			OnNewLeader: func(identity string) {
				log.Infof("some new guy is the leader now")
			},
		}

		elector.Run(&ctx)

	case __STANDALONE__:
		fmt.Println("(Check dev mode): Running in `stadalone` mode!")
	}
}

func runInProdMode() {
	value := utils.GetEnv(__PROD_MODE__, __STANDALONE__)

	switch value {
	case __SHARDED__:
		log.Infof("(Check prod mode): Running in `sharded` mode!")
		ctx := context.Background()

		config, err := rest.InClusterConfig()
		if err != nil {
			panic(err)
		}

		id := buildHostNameForK8sShard()
		lockNamespace := utils.GetEnv(__K8S_LEASE_NAMESAPCE__, "default")
		lockName := utils.GetEnv(__K8S_LEASE_NAME__, "orange-leader-election-lock")

		elector := &elector.LeaderElector{
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
			Name:          lockName,
			Namespace:     lockNamespace,
			Host:          id,
			Config:        config,
			OnStartedLeading: func(ctx context.Context) {
				log.Infof("I become leader")
			},
			OnStoppedLeading: func() {
				log.Infof("stpping down as a leader")
			},
			OnNewLeader: func(identity string) {
				log.Infof("some new guy is the leader now")
			},
		}

		elector.Run(&ctx)

	case __STANDALONE__:
		log.Infof("(Check prod mode): Running in `stadalone` mode!")
	}
}

func buildHostNameForK8sShard() string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local",
		utils.GetEnv(__K8S_POD_NAME__, "", true),
		utils.GetEnv(__K8S_SERVICE_NAME__, "", true),
		utils.GetEnv(__K8S_LEASE_NAMESAPCE__, "", true),
	)
}
