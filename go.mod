module github.com/BrobridgeOrg/gravity-controller

go 1.13

require (
	github.com/BrobridgeOrg/broc v0.0.2
	github.com/BrobridgeOrg/gravity-api v0.2.26
	github.com/BrobridgeOrg/gravity-sdk v1.0.4
	github.com/golang/protobuf v1.5.2
	github.com/nats-io/nats.go v1.16.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.7.1
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

//replace github.com/BrobridgeOrg/gravity-sdk => ../gravity-sdk

//replace github.com/BrobridgeOrg/broc => ../../broc
