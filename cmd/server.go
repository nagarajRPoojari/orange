package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nagarajRPoojari/orange/internal/elector"
	"github.com/nagarajRPoojari/orange/internal/utils"
	"github.com/nagarajRPoojari/orange/net/server"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	__BUILD_MODE__          = "__BUILD_MODE__"
	__DEV_MODE__            = "__DEV_MODE__"
	__PROD_MODE__           = "__PROD_MODE__"
	__HOST_ID__             = "__HOST_ID__"
	__K8S_LEASE_NAMESAPCE__ = "__K8S_LEASE_NAMESAPCE__"
	__K8S_LEASE_NAME__      = "__K8S_LEASE_NAME__"

	__SHARDED__    = "sharded"
	__STANDALONE__ = "standalone"
	__DEV__        = "dev"
	__PROD__       = "prod"
)

func init() {
	rootCmd.AddCommand(serverCmd)

	// Add flags
	serverCmd.Flags().StringVarP(&ServerPort, "port", "p", "8080", "Port to run the server ")
	serverCmd.Flags().StringVarP(&ServerAddress, "address", "a", "127.0.0.1", "Address to bind the server to")
}

var (
	ServerPort    string
	ServerAddress string
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Run: func(cmd *cobra.Command, args []string) {
		checkBuildMode()
		fmt.Printf("Starting server at %s:%s...\n", ServerAddress, ServerPort)
		// log.Disable()
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

		p, _ := strconv.ParseInt(ServerPort, 10, 0)
		s := server.NewServer(ServerAddress, p)
		go s.Run()

		<-stop
		fmt.Println("\nShutting down server...")

		s.Stop()
	},
}

func checkBuildMode() {
	value := utils.GetEnv(__BUILD_MODE__, __DEV__)

	switch value {
	case __DEV__:
		runInDevMode()
	case __PROD__:
		runInProdMode()
	}
}

func runInDevMode() {
	value := utils.GetEnv(__DEV_MODE__, __SHARDED__)

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
		fmt.Println("(Check prod mode): Running in `sharded` mode!")
		ctx := context.Background()

		config, err := rest.InClusterConfig()
		if err != nil {
			panic(err)
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
		fmt.Println("(Check prod mode): Running in `stadalone` mode!")
	}
}
