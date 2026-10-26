package platform

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"iot/internal/contracts"
)

type ManagementParameters struct {
	Resource  string
	TenantID  string
	DeviceID  string
	CommandID string
	Query     url.Values
}

type managementParametersKey struct{}

// ManagementRequest is the one path/query interpretation used by authorization
// and handlers. Tenant and device identities are never taken from JSON or query.
func ManagementRequest(r *http.Request) (ManagementParameters, error) {
	if p, ok := r.Context().Value(managementParametersKey{}).(ManagementParameters); ok {
		return p, nil
	}
	p := ManagementParameters{}
	if !strings.HasPrefix(r.URL.Path, "/api/v1/") {
		return p, ErrNotFound
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/"), "/")
	if len(parts) == 1 && parts[0] == "tenants" {
		p.Resource = "tenants"
	} else {
		if len(parts) < 3 || parts[0] != "tenants" || !contracts.IsValidTenantID(parts[1]) {
			return p, fmt.Errorf("%w: invalid tenant resource path", contracts.ErrInvalidEnvelope)
		}
		p.TenantID = parts[1]
		switch parts[2] {
		case "devices":
			if len(parts) == 3 {
				p.Resource = "devices"
				break
			}
			p.DeviceID = parts[3]
			if err := contracts.ValidateDeviceIdentity(p.TenantID, p.DeviceID); err != nil {
				return p, err
			}
			switch {
			case len(parts) == 4:
				p.Resource = "device"
			case len(parts) == 5 && (parts[4] == "status" || parts[4] == "telemetry" || parts[4] == "commands"):
				p.Resource = parts[4]
				if parts[4] == "commands" {
					p.Resource = "deviceCommands"
				}
			case len(parts) == 7 && parts[4] == "commands" && parts[6] == "acknowledgements":
				p.Resource = "acknowledgements"
				p.CommandID = parts[5]
			default:
				return p, ErrNotFound
			}
		case "commands":
			if len(parts) == 3 {
				p.Resource = "commands"
			} else if len(parts) == 4 {
				p.Resource = "command"
				p.CommandID = parts[3]
			} else {
				return p, ErrNotFound
			}
		default:
			return p, ErrNotFound
		}
	}
	if p.CommandID != "" {
		if err := contracts.ValidateCommandID(p.CommandID); err != nil {
			return p, err
		}
	}
	allowed := map[string]bool{}
	if r.Method == http.MethodGet {
		switch p.Resource {
		case "tenants", "devices", "telemetry", "commands", "deviceCommands":
			allowed["pageSize"] = true
			allowed["cursor"] = true
		}
		if p.Resource == "telemetry" || p.Resource == "commands" || p.Resource == "deviceCommands" {
			for _, key := range []string{"startTime", "endTime", "sort"} {
				allowed[key] = true
			}
		}
		if p.Resource == "telemetry" {
			allowed["type"] = true
		}
		if p.Resource == "commands" || p.Resource == "deviceCommands" {
			allowed["status"] = true
		}
		if p.Resource == "commands" {
			allowed["deviceId"] = true
		}
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return p, fmt.Errorf("invalid query encoding")
	}
	for key, values := range query {
		if !allowed[key] {
			return p, fmt.Errorf("unsupported query parameter %q", key)
		}
		if len(values) != 1 || values[0] == "" {
			return p, fmt.Errorf("query parameter %q must have exactly one non-empty value", key)
		}
	}
	if raw := query.Get("pageSize"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxPageSize {
			return p, fmt.Errorf("pageSize must be an integer between 1 and %d", maxPageSize)
		}
	}
	if len(query.Get("cursor")) > 4096 {
		return p, fmt.Errorf("cursor exceeds 4096 bytes")
	}
	if len(r.Header.Values("Idempotency-Key")) > 1 {
		return p, fmt.Errorf("Idempotency-Key must have exactly one value")
	}
	p.Query = query
	return p, nil
}

func withManagementParameters(r *http.Request, p ManagementParameters) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), managementParametersKey{}, p))
}
