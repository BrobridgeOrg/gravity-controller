module github.com/BrobridgeOrg/gravity-controller

go 1.13

require (
	github.com/BrobridgeOrg/gravity-dispatcher v0.0.0-20220206181110-2ea65aa048be
	github.com/BrobridgeOrg/gravity-sdk v0.0.50
	github.com/BrobridgeOrg/gravity-snapshot v0.0.0-20220213082459-fd38b9058d95
	github.com/BrobridgeOrg/schemer v0.0.10
	github.com/BrobridgeOrg/sequential-data-flow v0.0.1
	github.com/d5/tengo v1.24.8
	github.com/google/uuid v1.1.2
	github.com/json-iterator/go v1.1.12
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/nats-io/nats.go v1.13.1-0.20220121202836-972a071d373d
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	go.uber.org/fx v1.16.0
	go.uber.org/zap v1.17.0
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 // indirect
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api

replace github.com/BrobridgeOrg/gravity-sdk => ../../gravity-sdk

//replace github.com/BrobridgeOrg/broc => ../../broc
