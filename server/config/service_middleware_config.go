// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import "github.com/tikv/pd/pkg/ratelimit"

const (
	defaultEnableAuditMiddleware     = true
	defaultEnableRateLimitMiddleware = false
)

// ServiceMiddlewareConfig is the configuration for PD Service middleware.
type ServiceMiddlewareConfig struct {
	AuditConfig     `json:"audit"`
	RateLimitConfig `json:"rate-limit"`
}

// NewServiceMiddlewareConfig returns a new service middleware config
func NewServiceMiddlewareConfig() *ServiceMiddlewareConfig {
	audit := AuditConfig{
		EnableAudit: defaultEnableAuditMiddleware,
	}
	ratelimit := RateLimitConfig{
		EnableRateLimit: defaultEnableRateLimitMiddleware,
		LimiterConfig:   make(map[string]ratelimit.DimensionConfig),
	}
	cfg := &ServiceMiddlewareConfig{
		AuditConfig:     audit,
		RateLimitConfig: ratelimit,
	}
	return cfg
}

// Clone returns a cloned service middleware configuration.
func (c *ServiceMiddlewareConfig) Clone() *ServiceMiddlewareConfig {
	cfg := *c
	return &cfg
}

// AuditConfig is the configuration for audit
type AuditConfig struct {
	// EnableAudit controls the switch of the audit middleware
	EnableAudit bool `json:"enable-audit,string"`
}

// Clone returns a cloned audit config.
func (c *AuditConfig) Clone() *AuditConfig {
	cfg := *c
	return &cfg
}

// RateLimitConfig is the configuration for rate limit
type RateLimitConfig struct {
	// EnableRateLimit controls the switch of the rate limit middleware
	EnableRateLimit bool `json:"enable-rate-limit,string"`
	// RateLimitConfig is the config of rate limit middleware
	LimiterConfig map[string]ratelimit.DimensionConfig `json:"limiter-config"`
}

// Clone returns a cloned rate limit config.
func (c *RateLimitConfig) Clone() *RateLimitConfig {
	cfg := *c
	return &cfg
}
