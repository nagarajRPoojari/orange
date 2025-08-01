package client

import (
	"context"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/nagarajRPoojari/orange/pkg/adapter"
	pb "github.com/nagarajRPoojari/orange/pkg/proto/ops"
	"github.com/nagarajRPoojari/orange/pkg/query"
	"google.golang.org/grpc"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.OpsClient
}

func NewClient() *Client {
	addr := "localhost:50051"
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial to server at %s", addr)
	}
	client := pb.NewOpsClient(conn)
	return &Client{
		conn:   conn,
		client: client,
	}
}

func (t *Client) Create(op *query.CreateOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	createAdapter := &adapter.CreateOpAdapter{Native: op}

	_, err := t.client.Create(ctx, createAdapter.ToProtobuf())
	if err != nil {
		return err
	}
	return nil
}

func (t *Client) Insert(op *query.InsertOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	insertAdapter := &adapter.InsertOpAdapter{Native: op}

	_, err := t.client.Insert(ctx, insertAdapter.ToProtobuf())
	if err != nil {
		return err
	}
	return nil
}

func (t *Client) Select(op *query.SelectOp) (*pb.SelectRes, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	selectAdapter := &adapter.SelectOpAdapter{Native: op}

	resp, err := t.client.Select(ctx, selectAdapter.ToProtobuf())
	if err != nil {
		return nil, err
	}

	return resp, nil
}
