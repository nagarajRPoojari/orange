package server

import (
	"context"
	"fmt"
	"net"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"

	"github.com/nagarajRPoojari/orange/internal/config"
	odb "github.com/nagarajRPoojari/orange/internal/db"
	"github.com/nagarajRPoojari/orange/pkg/adapter"
	pb "github.com/nagarajRPoojari/orange/pkg/proto/ops"
	"google.golang.org/grpc"
)

type OpsServer struct {
	pb.UnimplementedOpsServer
	db *odb.Oragedb
}

type Server struct {
	db     *odb.Oragedb
	ctx    *context.Context
	cancel context.CancelFunc
	addr   string
}

func NewServer(addr string, port int64) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		db:     odb.NewOrangedb(ctx, config.GetConfig()),
		ctx:    &ctx,
		cancel: cancel,
		addr:   fmt.Sprintf("%s:%d", addr, port),
	}
}

func (t *Server) Run() {
	lis, err := net.Listen("tcp", t.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOpsServer(grpcServer, &OpsServer{db: t.db})

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
	if err := t.db.InsertDoc(*op); err != nil {
		return nil, err
	}
	return &pb.InsertRes{Status: true}, nil
}

func (t *OpsServer) Delete(ctx context.Context, req *pb.DeleteReq) (*pb.DeleteRes, error) {
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
