package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	Outbounds                 []string                   `json:"outbounds"`
	Default                   string                     `json:"default,omitempty"`
	LoadBalance               SelectorLoadBalanceOptions `json:"load_balance,omitempty"`
	InterruptExistConnections bool                       `json:"interrupt_exist_connections,omitempty"`
}

type SelectorLoadBalanceOptions struct {
	Enabled   bool   `json:"enabled,omitempty"`
	Instances int    `json:"instances,omitempty"`
	Strategy  string `json:"strategy,omitempty"`
}

type URLTestOutboundOptions struct {
	Outbounds                 []string           `json:"outbounds"`
	URL                       string             `json:"url,omitempty"`
	Interval                  badoption.Duration `json:"interval,omitempty"`
	Tolerance                 uint16             `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool               `json:"interrupt_exist_connections,omitempty"`
}
