package middleware

import (
	"github.com/BrobridgeOrg/broc"
	packet_pb "github.com/BrobridgeOrg/gravity-api/packet"
	"github.com/golang/protobuf/proto"
)

func (m *Middleware) PacketHandler(ctx *broc.Context) (interface{}, error) {

	var packet packet_pb.Packet
	err := proto.Unmarshal(ctx.Get("request").([]byte), &packet)
	if err != nil {
		// invalid request
		return nil, nil
	}

	ctx.Set("request", &packet)

	return ctx.Next()
}
