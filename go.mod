module github.com/BrobridgeOrg/gravity-controller

go 1.13

require (
	github.com/BrobridgeOrg/EventStore v0.0.5 // indirect
	github.com/BrobridgeOrg/gravity-api v0.2.6
	github.com/cfsghost/grpc-connection-pool v0.0.0-20200903182758-f64b83c701d7
	github.com/golang/protobuf v1.4.2
	github.com/konsorten/go-windows-terminal-sequences v1.0.3 // indirect
	github.com/nats-io/nats-server/v2 v2.1.7 // indirect
	github.com/nats-io/nats.go v1.10.0
	github.com/sirupsen/logrus v1.7.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.4.0 // indirect
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	golang.org/x/sys v0.0.0-20200202164722-d101bd2416d5 // indirect
	google.golang.org/grpc v1.31.1
	google.golang.org/grpc/examples v0.0.0-20200807164945-d3e3e7a46f57 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
