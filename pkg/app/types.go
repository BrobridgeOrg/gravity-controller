package app

import (
	"github.com/BrobridgeOrg/gravity-controller/pkg/controller"
)

type App interface {
	GetController() controller.Controller
}
