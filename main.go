package main

import (
	"os"

	"github.com/BrobridgeOrg/gravity-controller/pkg/configs"
	"github.com/BrobridgeOrg/gravity-controller/pkg/connector"
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller"
	"github.com/BrobridgeOrg/gravity-controller/pkg/logger"
	"github.com/spf13/cobra"

	"go.uber.org/fx"
)

var config *configs.Config
var events []string

var rootCmd = &cobra.Command{
	Use:   "gravity-controller",
	Short: "Gravity Component to manage cluster",
	Long: `gravity-controller a component to manage cluster.
This application can be used to manage adapter, subscriber and authorization.`,
	RunE: func(cmd *cobra.Command, args []string) error {

		if err := run(); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	config = configs.GetConfig()

	rootCmd.Flags().StringSliceVar(&events, "events", []string{}, "Specify events for watching")
}

func main() {

	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func run() error {

	config.AddEvents(events)

	fx.New(
		fx.Supply(config),
		fx.Provide(
			logger.GetLogger,
			connector.New,
		),
		fx.Invoke(controller.New),
		fx.NopLogger,
	).Run()

	return nil
}
