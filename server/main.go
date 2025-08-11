package server

import (
	"github.com/nagarajRPoojari/orange/internal/utils"
	grpcserver "github.com/nagarajRPoojari/orange/net/server"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
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
	// only required when running sharded cluster
	__REPLICA_COUNT__ = "__REPLICA_COUNT__"

	// expected only when running sharded cluster in prod
	__K8S_NAMESAPCE__   = "__K8S_NAMESAPCE__"
	__K8S_LEASE_NAME__  = "__K8S_LEASE_NAME__"
	__K8S_POD_NAME__    = "__K8S_POD_NAME__"
	__k8S__SHARD_NAME__ = "__k8S__SHARD_NAME__"

	// replication related vars
	__TURNON_REPLICATION__ = "__TURNON_REPLICATION__"
	__REPLICATION_TYPE__   = "__REPLICATION_TYPE__"
	__ACK_LEVEL__          = "__ACK_LEVEL__"

	// mode values
	__SHARDED__    = "sharded"
	__STANDALONE__ = "standalone"

	__DEV__    = "dev"
	__PROD__   = "prod"
	__SYNC__   = "sync"
	__ASYNC__  = "async"
	__QUORUM__ = "quorum"
	__ALL__    = "all"
)

var grpcSeverInstance *grpcserver.Server

func Start(addr string, port int64) {
	value := utils.GetEnv(__BUILD_MODE__, __DEV__)

	switch value {
	case __DEV__:
		runInDevMode(addr, port)
	case __PROD__:
		log.Disable()
		runInProdMode(addr, port)
	}
}

func Stop() {
	if grpcSeverInstance != nil {
		grpcSeverInstance.Stop()
		log.Infof("server stopped successfully.")
	} else {
		log.Warnf("Attempted to stop server, but it was not running.")
	}
}

func runInDevMode(addr string, port int64) {
	value := utils.GetEnv(__DEV_MODE__, __STANDALONE__)

	switch value {
	case __SHARDED__:
		log.Infof("Running in DEV environment")
		log.Infof("Running in `sharded` mode!")
		replication := utils.GetEnv(__TURNON_REPLICATION__, false)
		if !replication {
			grpcSeverInstance = grpcserver.NewServer(addr, port, &grpcserver.ReplicationOpts{})
			go grpcSeverInstance.Run()
			return
		}
		level := utils.GetEnv(__REPLICATION_TYPE__, __SYNC__)
		replicas := utils.GetEnv(__REPLICA_COUNT__, 0)

		grpcSeverInstance = grpcserver.NewServer(addr, port, &grpcserver.ReplicationOpts{
			TurnOnReplication: true,
			ReplicationType:   grpcserver.ReplicationType(level),
			Replicas:          replicas,
			AckLevel:          grpcserver.AckLevel(level),
		})
		go grpcSeverInstance.Run()

	case __STANDALONE__:
		log.Infof("Running in DEV environment")
		log.Infof("Running in `standalone` mode!")

		grpcSeverInstance = grpcserver.NewServer(addr, port, &grpcserver.ReplicationOpts{})
		go grpcSeverInstance.Run()
	}
}

func runInProdMode(addr string, port int64) {
	value := utils.GetEnv(__PROD_MODE__, __STANDALONE__)

	switch value {
	case __SHARDED__:
		log.Infof("Running in PROD environment")
		log.Infof("Running in `sharded` mode!")
		replication := utils.GetEnv(__TURNON_REPLICATION__, false)
		if !replication {
			grpcSeverInstance = grpcserver.NewServer(addr, port, &grpcserver.ReplicationOpts{})
			go grpcSeverInstance.Run()
			return
		}
		level := utils.GetEnv(__REPLICATION_TYPE__, __SYNC__)
		replicas := utils.GetEnv(__REPLICA_COUNT__, 0, true)

		grpcSeverInstance = grpcserver.NewServer(addr, port, &grpcserver.ReplicationOpts{
			TurnOnReplication: true,
			ReplicationType:   grpcserver.ReplicationType(level),
			Replicas:          replicas,
			AckLevel:          grpcserver.AckLevel(level),
		})
		go grpcSeverInstance.Run()

	case __STANDALONE__:
		log.Infof("Running in PROD environment")
		log.Infof("Running in `standalone` mode!")

		grpcSeverInstance = grpcserver.NewServer(addr, port, &grpcserver.ReplicationOpts{})
		go grpcSeverInstance.Run()
	}
}
