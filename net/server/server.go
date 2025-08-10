package server

import (
	"context"
	"fmt"
	"net"

	"github.com/nagarajRPoojari/orange/net/client"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"

	"github.com/nagarajRPoojari/orange/internal/config"
	odb "github.com/nagarajRPoojari/orange/internal/db"
	"github.com/nagarajRPoojari/orange/internal/utils"
	"github.com/nagarajRPoojari/orange/pkg/adapter"
	pb "github.com/nagarajRPoojari/orange/pkg/proto/ops"
	"google.golang.org/grpc"
)

const (
	__K8S_NAMESAPCE__  = "__K8S_NAMESAPCE__"
	__K8S_POD_NAME__   = "__K8S_POD_NAME__"
	__K8S_SHARD_NAME__ = "__K8S_SHARD_NAME__"
)

type OpsServer struct {
	pb.UnimplementedOpsServer
	db       *odb.Oragedb
	replOpts *ReplicationOpts
}

type ReplicationType string

const (
	Synchronous  = "sync"
	Asynchronous = "async"
)

type AckLevel string

const (
	quorum = "quorum" // not implemented yet
	all    = "all"
)

type ReplicationOpts struct {
	TurnOnReplication bool
	ReplicationType   ReplicationType
	AckLevel          AckLevel
	Replicas          int
	replicaCLientList []*client.Client
}

type Server struct {
	db     *odb.Oragedb
	ctx    *context.Context
	cancel context.CancelFunc
	addr   string

	replicationOpts *ReplicationOpts
}

func NewServer(addr string, port int64, replicationOpts *ReplicationOpts) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	replicationOpts.replicaCLientList = buildHostNameForK8sShards(replicationOpts.Replicas)
	return &Server{
		db:              odb.NewOrangedb(ctx, config.GetConfig()),
		ctx:             &ctx,
		cancel:          cancel,
		addr:            fmt.Sprintf("%s:%d", addr, port),
		replicationOpts: replicationOpts,
	}
}

func (t *Server) Run() {
	lis, err := net.Listen("tcp", t.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOpsServer(grpcServer, &OpsServer{db: t.db, replOpts: t.replicationOpts})

	log.Infof("gRPC server listening on %s", t.addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (t *Server) Stop() {
	t.cancel()
}

func (t *OpsServer) Create(ctx context.Context, req *pb.CreateReq) (*pb.CreatRes, error) {
	copAdapter := &adapter.CreateOpAdapter{Pb: req}
	op := copAdapter.ToNative()
	if err := t.db.CreateCollection(*op); err != nil {
		return nil, err
	}
	return &pb.CreatRes{Status: true}, nil
}

func (t *OpsServer) Insert(ctx context.Context, req *pb.InsertReq) (*pb.InsertRes, error) {
	insertAdapter := &adapter.InsertOpAdapter{Pb: req}
	op := insertAdapter.ToNative()
	if t.replOpts.TurnOnReplication {
		for _, cl := range t.replOpts.replicaCLientList {
			if err := cl.SecondaryInsert(op); err != nil {
				return &pb.InsertRes{Status: false}, err
			}
		}
	}

	if err := t.db.InsertDoc(*op); err != nil {
		return nil, err
	}

	return &pb.InsertRes{Status: true}, nil
}

func (t *OpsServer) SecondaryInsert(ctx context.Context, req *pb.InsertReq) (*pb.InsertRes, error) {
	insertAdapter := &adapter.InsertOpAdapter{Pb: req}
	op := insertAdapter.ToNative()
	if err := t.db.InsertDoc(*op); err != nil {
		return nil, err
	}
	return &pb.InsertRes{Status: true}, nil
}

func (t *OpsServer) Delete(ctx context.Context, req *pb.DeleteReq) (*pb.DeleteRes, error) {
	deleteAdapter := &adapter.DeleteOpAdapter{Pb: req}
	op := deleteAdapter.ToNative()
	if t.replOpts.TurnOnReplication {
		for _, cl := range t.replOpts.replicaCLientList {
			if err := cl.SecondaryDelete(op); err != nil {
				return &pb.DeleteRes{Status: false}, err
			}
		}
	}
	if err := t.db.DeleteDoc(*op); err != nil {
		return nil, err
	}
	return &pb.DeleteRes{Status: true}, nil
}

func (t *OpsServer) SecondaryDelete(ctx context.Context, req *pb.DeleteReq) (*pb.DeleteRes, error) {
	deleteAdapter := &adapter.DeleteOpAdapter{Pb: req}
	op := deleteAdapter.ToNative()
	if err := t.db.DeleteDoc(*op); err != nil {
		return nil, err
	}
	return &pb.DeleteRes{Status: true}, nil
}

func (t *OpsServer) Select(ctx context.Context, req *pb.SelectReq) (*pb.SelectRes, error) {
	selectAdpater := &adapter.SelectOpAdapter{Pb: req}
	op := selectAdpater.ToNative()
	val, err := t.db.GetDoc(*op)
	if err != nil {
		return nil, err
	}
	jsonAdapter := &adapter.JsonAdapter{Native: &val}
	return &pb.SelectRes{Data: *jsonAdapter.ToProtobuf()}, nil
}

func buildHostNameForK8sShards(replicas int) []*client.Client {
	shards := make([]*client.Client, 0)
	for i := range replicas {
		shardName := utils.GetEnv(__K8S_SHARD_NAME__, "", true)
		host := fmt.Sprintf("%s-%d", shardName, i)

		if host == utils.GetEnv(__K8S_POD_NAME__, "", true) {
			continue
		}
		addr := fmt.Sprintf("%s.%s.%s.svc.cluster.local",
			host,
			shardName,
			utils.GetEnv(__K8S_NAMESAPCE__, "", true),
		)
		log.Infof("connecting to %s at %s ", shardName, addr)
		// other replicas might not be up when current shard is pinging, so
		// retry with exponential backoff for eventual connection.
		cl := client.NewClient(addr, 52001, 10)
		shards = append(shards, cl)
	}
	return shards
}
