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

func NewClient(addr string, port int64, retry ...int) *Client {
	address := fmt.Sprintf("%s:%d", addr, port)

	// Default retry count
	retries := 1
	if len(retry) > 0 {
		retries = retry[0]
	}

	var conn *grpc.ClientConn
	var err error

	for attempt := 0; attempt <= retries; attempt++ {
		conn, err = grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}

		log.Warnf("failed to connect to %s (attempt %d/%d): %v", address, attempt+1, retries+1, err)
		time.Sleep(time.Duration(500*(1<<attempt)) * time.Millisecond) // exponential backoff
	}

	if err != nil {
		log.Fatalf("could not connect to server at %s after %d attempts", address, retries+1)
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

func (t *Client) SecondaryInsert(op *oql.InsertOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	insertAdapter := &adapter.InsertOpAdapter{Native: op}

	_, err := t.client.SecondaryInsert(ctx, insertAdapter.ToProtobuf())
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

func (t *Client) Delete(op *oql.DeleteOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	deleteAdapter := &adapter.DeleteOpAdapter{Native: op}

	_, err := t.client.Delete(ctx, deleteAdapter.ToProtobuf())
	if err != nil {
		return err
	}

	return nil
}

func (t *Client) SecondaryDelete(op *oql.DeleteOp) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	deleteAdapter := &adapter.DeleteOpAdapter{Native: op}

	_, err := t.client.SecondaryDelete(ctx, deleteAdapter.ToProtobuf())
	if err != nil {
		return err
	}

	return nil
}
