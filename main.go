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
	"github.com/nagarajRPoojari/orange/internal/utils"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
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

		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			panic(err)
		}

		id := utils.GetEnv(__HOST_ID__, uuid.NewString())
		lockNamespace := utils.GetEnv(__K8S_LEASE_NAMESAPCE__, "default")

		lockName := utils.GetEnv(__K8S_LEASE_NAME__, "orange-leader-election-lock")

		lock := &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      lockName,
				Namespace: lockNamespace,
			},
			Client: clientset.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: id,
			},
		}

		go func() {
			leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
				Lock:          lock,
				LeaseDuration: 15 * time.Second,
				RenewDeadline: 10 * time.Second,
				RetryPeriod:   2 * time.Second,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {
						fmt.Println("I am the leader now")
						// Do leader stuff
					},
					OnStoppedLeading: func() {
						fmt.Println("I am no longer the leader")
					},
					OnNewLeader: func(identity string) {
						if identity == id {
							return
						}
						fmt.Printf("New leader elected: %s\n", identity)
					},
				},
				ReleaseOnCancel: true,
				Name:            "orange",
			})
		}()

	case __STANDALONE__:
		fmt.Println("(Check dev mode): Running in `stadalone` mode!")
	}
}

func runInProdMode() {
	value := utils.GetEnv(__PROD_MODE__, __STANDALONE__)

	switch value {
	case __SHARDED__:
		fmt.Println("(Check prod mode): Running in `sharded` mode!")

	case __STANDALONE__:
		fmt.Println("(Check prod mode): Running in `stadalone` mode!")
	}
}
