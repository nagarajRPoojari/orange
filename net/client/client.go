package client

import (
	"context"
	"fmt"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/nagarajRPoojari/orange/pkg/adapter"
	"github.com/nagarajRPoojari/orange/pkg/oql"
	pb "github.com/nagarajRPoojari/orange/pkg/proto/ops"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.OpsClient
}

func NewClient(addr string, port int64) *Client {
	address := fmt.Sprintf("%s:%d", addr, port)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial to server at %s", address)
	}
	client := pb.NewOpsClient(conn)
	return &Client{
		conn:   conn,
		client: client,
	}
}

func (t *Client) Create(op *oql.CreateOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	createAdapter := &adapter.CreateOpAdapter{Native: op}

	_, err := t.client.Create(ctx, createAdapter.ToProtobuf())
	if err != nil {
		return err
	}
	return nil
}

func (t *Client) Insert(op *oql.InsertOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	insertAdapter := &adapter.InsertOpAdapter{Native: op}

	_, err := t.client.Insert(ctx, insertAdapter.ToProtobuf())
	if err != nil {
		return err
	}
	return nil
}

func (t *Client) Select(op *oql.SelectOp) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	selectAdapter := &adapter.SelectOpAdapter{Native: op}

	resp, err := t.client.Select(ctx, selectAdapter.ToProtobuf())
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}
