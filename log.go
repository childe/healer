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

// SetLogger passes sets the logger in healer
func SetLogger(l logr.Logger) {
	logger = l
}

// GetLogger returns the logger in healer lib
func GetLogger() logr.Logger {
	return logger
}
