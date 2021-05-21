module github.com/BrobridgeOrg/gravity-controller

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.2.14
	github.com/BrobridgeOrg/gravity-sdk v0.0.8
	github.com/cfsghost/grpc-connection-pool v0.6.0
	github.com/golang/protobuf v1.4.2
	github.com/konsorten/go-windows-terminal-sequences v1.0.3 // indirect
	github.com/nats-io/nats.go v1.10.0
	github.com/sirupsen/logrus v1.7.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	golang.org/x/net v0.0.0-20200625001655-4c5254603344
	google.golang.org/grpc v1.32.0
	google.golang.org/grpc/examples v0.0.0-20200807164945-d3e3e7a46f57 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
//replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk
