package adapter

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nagarajRPoojari/orange/pkg/oql"
	pb "github.com/nagarajRPoojari/orange/pkg/proto/ops"
	"google.golang.org/protobuf/types/known/structpb"
)

type CreateOpAdapter struct {
	Native *oql.CreateOp
	Pb     *pb.CreateReq
}

func (t *CreateOpAdapter) ToProtobuf() *pb.CreateReq {
	schemaPb, err := structpb.NewStruct(t.Native.Schema)
	if err != nil {
		fmt.Printf("PROBLEM HERE %v\n", err)
		fmt.Printf("::: %v\n", t.Native)
		log.Fatalf("failed to create structpb.Struct: %v", err)
	}
	return &pb.CreateReq{
		Document: t.Native.Document,
		Schema:   schemaPb,
	}
}

func (t *CreateOpAdapter) ToNative() *oql.CreateOp {
	return &oql.CreateOp{
		Document: t.Pb.Document,
		Schema:   t.Pb.Schema.AsMap(),
	}
}

type InsertOpAdapter struct {
	Native *oql.InsertOp
	Pb     *pb.InsertReq
}

func (t *InsertOpAdapter) ToProtobuf() *pb.InsertReq {
	valPb, err := structpb.NewStruct(t.Native.Value)
	if err != nil {
		log.Fatalf("failed to create structpb.Struct: %v", err)
	}
	return &pb.InsertReq{
		Document: t.Native.Document,
		Value:    valPb,
	}
}

func (t *InsertOpAdapter) ToNative() *oql.InsertOp {
	return &oql.InsertOp{
		Document: t.Pb.Document,
		Value:    t.Pb.Value.AsMap(),
	}
}

type SelectOpAdapter struct {
	Native *oql.SelectOp
	Pb     *pb.SelectReq
}

func (t *SelectOpAdapter) ToProtobuf() *pb.SelectReq {
	return &pb.SelectReq{
		Document: t.Native.Document,
		Columns:  t.Native.Columns,
		Id:       t.Native.ID,
	}
}

func (t *SelectOpAdapter) ToNative() *oql.SelectOp {
	return &oql.SelectOp{
		Document: t.Pb.Document,
		Columns:  t.Pb.Columns,
		ID:       t.Pb.Id,
	}
}

type DeleteOpAdapter struct {
	Native *oql.DeleteOp
	Pb     *pb.DeleteReq
}

func (t *DeleteOpAdapter) ToProtobuf() *pb.DeleteReq {
	return &pb.DeleteReq{
		Document: t.Native.Document,
		Id:       t.Native.ID,
	}
}

func (t *DeleteOpAdapter) ToNative() *oql.DeleteOp {
	return &oql.DeleteOp{
		Document: t.Pb.Document,
		ID:       t.Pb.Id,
	}
}

type JsonAdapter struct {
	Native *map[string]interface{}
}

func (t *JsonAdapter) ToProtobuf() *[]byte {
	data, err := json.Marshal(t.Native)
	if err != nil {
		log.Fatalf("fatal")
	}
	return &data
}
