/*
Copyright © 2025 nagarajRPoojari

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/nagarajRPoojari/orange/cmd"
	src "github.com/nagarajRPoojari/orange/internal"
	"github.com/nagarajRPoojari/orange/internal/elector"
	"github.com/nagarajRPoojari/orange/internal/utils"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
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

func main() {
	fmt.Printf("Running project: `%s`\n", src.ProjectName())
	checkBuildMode()
	cmd.Execute()
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
