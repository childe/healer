package healer

import (
	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
)

// logger is Klogr by default, user can set a different logger using SetLogger
var logger logr.Logger

func init() {
	logger = klog.NewKlogr().WithName("healer")
}

// SetLogger sets the logger to be used in the package.
func SetLogger(l logr.Logger) {
	logger = l
}
