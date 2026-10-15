package libv2ray

// Tropa additions to the core controller. Kept in this separate file so upstream changes merge
// without conflicts; the Tropa CI overlays tropa_*.go onto the library commit pinned by v2rayNG.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	corenet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/infra/conf"
)

// tropaProbeResult is returned to Kotlin as JSON.
type tropaProbeResult struct {
	OK        bool   `json:"ok"`
	Status    int    `json:"status"`
	LatencyMs int64  `json:"latencyMs"` // until the response headers arrived
	TotalMs   int64  `json:"totalMs"`   // until the body was read
	Bytes     int64  `json:"bytes"`
	Error     string `json:"error,omitempty"`
}

func (x *CoreController) instance() *core.Instance {
	x.coreMutex.Lock()
	defer x.coreMutex.Unlock()
	return x.coreInstance
}

// TropaProbe fetches url through the running core and reports how it went (JSON).
//
// With a non-empty outboundTag the request is forced through that outbound, bypassing routing,
// which checks one server of a balancer at a time; with an empty tag it follows the routing.
// The body is read up to maxBytes so a channel that stalls after the first kilobytes is caught.
// Any HTTP status below 400 counts as success (generate_204 answers 204).
func (x *CoreController) TropaProbe(outboundTag string, url string, timeoutMs int64, maxBytes int64) string {
	result := tropaProbe(x.instance(), outboundTag, url, timeoutMs, maxBytes)
	data, _ := json.Marshal(result)
	return string(data)
}

func tropaProbe(inst *core.Instance, tag string, url string, timeoutMs int64, maxBytes int64) tropaProbeResult {
	if inst == nil {
		return tropaProbeResult{Error: "core is not running"}
	}
	if timeoutMs <= 0 {
		timeoutMs = 10000
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	transport := &http.Transport{
		DisableKeepAlives:   true,
		TLSHandshakeTimeout: time.Duration(timeoutMs) * time.Millisecond,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dest, err := corenet.ParseDestination(fmt.Sprintf("%s:%s", network, addr))
			if err != nil {
				return nil, err
			}
			if tag != "" {
				ctx = session.SetForcedOutboundTagToContext(ctx, tag)
			}
			return core.Dial(ctx, inst, dest)
		},
	}
	defer transport.CloseIdleConnections()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return tropaProbeResult{Error: err.Error()}
	}
	request.Header.Set("Cache-Control", "no-cache")

	start := time.Now()
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return tropaProbeResult{Error: err.Error(), TotalMs: time.Since(start).Milliseconds()}
	}
	defer response.Body.Close()
	latency := time.Since(start).Milliseconds()

	reader := io.Reader(response.Body)
	if maxBytes > 0 {
		reader = io.LimitReader(response.Body, maxBytes)
	}
	read, err := io.Copy(io.Discard, reader)
	result := tropaProbeResult{
		Status:    response.StatusCode,
		LatencyMs: latency,
		TotalMs:   time.Since(start).Milliseconds(),
		Bytes:     read,
	}
	switch {
	case err != nil:
		result.Error = err.Error()
	case response.StatusCode >= 400:
		result.Error = fmt.Sprintf("HTTP %d", response.StatusCode)
	default:
		result.OK = true
	}
	return result
}

func (x *CoreController) balancerOverrider() (routing.BalancerOverrider, error) {
	inst := x.instance()
	if inst == nil {
		return nil, errors.New("core is not running")
	}
	router, ok := inst.GetFeature(routing.RouterType()).(routing.Router)
	if !ok || router == nil {
		return nil, errors.New("router feature is not available")
	}
	overrider, ok := router.(routing.BalancerOverrider)
	if !ok {
		return nil, errors.New("router does not support balancer override")
	}
	return overrider, nil
}

// TropaOverrideBalancer sends new connections of balancerTag to target; open connections stay
// where they are. An empty target returns the choice to the balancer strategy.
func (x *CoreController) TropaOverrideBalancer(balancerTag string, target string) error {
	overrider, err := x.balancerOverrider()
	if err != nil {
		return err
	}
	return overrider.SetOverrideTarget(balancerTag, target)
}

// TropaBalancerTarget returns the current override target of balancerTag ("" when none).
func (x *CoreController) TropaBalancerTarget(balancerTag string) (string, error) {
	overrider, err := x.balancerOverrider()
	if err != nil {
		return "", err
	}
	return overrider.GetOverrideTarget(balancerTag)
}

// TropaAddOutbound adds an outbound (Xray JSON, one entry of "outbounds") to the running core.
// New servers of an updated subscription are checked and used this way without a restart, so
// open connections on the old servers are not cut; the tag must not be taken yet.
func (x *CoreController) TropaAddOutbound(outboundJSON string) error {
	inst := x.instance()
	if inst == nil {
		return errors.New("core is not running")
	}
	var detour conf.OutboundDetourConfig
	if err := json.Unmarshal([]byte(outboundJSON), &detour); err != nil {
		return err
	}
	config, err := detour.Build()
	if err != nil {
		return err
	}
	return core.AddOutboundHandler(inst, config)
}
