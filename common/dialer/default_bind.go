package dialer

import (
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/control"
)

type bindMode uint8

const (
	bindModeNone bindMode = iota
	bindModeExplicit
	bindModeAutoDetect
	bindModeProtect
)

type defaultBindPlan struct {
	mode4          bindMode
	mode6          bindMode
	interface4     string
	interface6     string
	enableStrategy bool
}

func resolveDefaultBindPlan(
	disableDefaultBind bool,
	autoDetect bool,
	hasPlatform bool,
	defaults adapter.NetworkOptions,
) defaultBindPlan {
	if disableDefaultBind {
		return defaultBindPlan{}
	}
	plan := defaultBindPlan{}
	if defaults.BindInterface != "" {
		plan.mode4 = bindModeExplicit
		plan.mode6 = bindModeExplicit
		plan.interface4 = defaults.BindInterface
		plan.interface6 = defaults.BindInterface
	}
	if defaults.BindIPv6Interface != "" {
		plan.mode6 = bindModeExplicit
		plan.interface6 = defaults.BindIPv6Interface
	}
	if !autoDetect {
		return plan
	}
	autoMode := bindModeAutoDetect
	if hasPlatform {
		autoMode = bindModeProtect
	}
	if plan.mode4 == bindModeNone {
		plan.mode4 = autoMode
		if autoMode == bindModeProtect {
			plan.enableStrategy = true
		}
	}
	if plan.mode6 == bindModeNone {
		plan.mode6 = autoMode
		if autoMode == bindModeProtect {
			plan.enableStrategy = true
		}
	}
	return plan
}

func bindControlForMode(mode bindMode, interfaceFinder control.InterfaceFinder, interfaceName string, autoDetect control.Func, protect control.Func) control.Func {
	switch mode {
	case bindModeExplicit:
		if interfaceName == "" {
			return nil
		}
		return control.BindToInterface(interfaceFinder, interfaceName, -1)
	case bindModeAutoDetect:
		return autoDetect
	case bindModeProtect:
		return protect
	default:
		return nil
	}
}
