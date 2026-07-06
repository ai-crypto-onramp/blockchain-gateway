package chain

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ChainConfig is the per-chain configuration loaded from environment variables
// (and, in later stages, optionally from YAML).
//
// It corresponds to the per-chain entry in the YAML config block shown in the
// README and to the env vars documented in the "Configuration" section:
//
//   - ChainID:        the chain identifier (e.g. "ethereum").
//   - RPCURLs:        RPC_URLS_<CHAIN> (comma-separated).
//   - WSURLs:         WS_URLS_<CHAIN> (comma-separated, optional).
//   - FinalityBlocks: FINALITY_BLOCKS_<CHAIN> (finality depth).
//   - GasStrategy:    GAS_STRATEGY_<CHAIN> override, else GAS_STRATEGY default.
type ChainConfig struct {
	// ChainID is the chain identifier, e.g. "ethereum", "polygon",
	// "solana", "bitcoin". Must be lowercase.
	ChainID string
	// RPCURLs is the list of JSON-RPC HTTP(S) endpoints for the chain.
	// At least one is required.
	RPCURLs []string
	// WSURLs is the list of WebSocket endpoints for the chain. Optional;
	// when empty, the tip follower falls back to polling
	// (CONFIRMATION_POLL_INTERVAL).
	WSURLs []string
	// FinalityBlocks is the chain's finality depth: a tx is never marked
	// finalized before this many confirmations.
	FinalityBlocks uint64
	// GasStrategy selects the fee/broadcast strategy for the chain, e.g.
	// "eip1559_dynamic", "eip1559_legacy_fallback", "solana_priority_fee",
	// "bitcoin_rbf".
	GasStrategy string
}

// DefaultGasStrategy is the fallback gas strategy when neither
// GAS_STRATEGY_<CHAIN> nor GAS_STRATEGY is set. It matches the README default
// for the GAS_STRATEGY env var.
const DefaultGasStrategy = "eip1559_dynamic"

// ConfigLoadError is a typed error returned by LoadConfigs when one or more
// per-chain configs are invalid (missing required env vars, bad numeric
// values, etc.). The Errors map is keyed by ChainID.
type ConfigLoadError struct {
	Errors map[string]error
}

func (e *ConfigLoadError) Error() string {
	if e == nil || len(e.Errors) == 0 {
		return "config: no errors"
	}
	parts := make([]string, 0, len(e.Errors))
	for chain, err := range e.Errors {
		parts = append(parts, fmt.Sprintf("%s: %v", chain, err))
	}
	return "config errors: " + strings.Join(parts, "; ")
}

// LoadConfigs reads per-chain configuration from environment variables and
// returns a populated []ChainConfig, one entry per chain listed in
// CHAINS_SUPPORTED.
//
// Env vars read (uppercase chain id substitution, e.g. ETHEREUM):
//
//   - CHAINS_SUPPORTED            (required) comma-separated chain ids.
//   - RPC_URLS_<CHAIN>            (required) comma-separated RPC urls.
//   - WS_URLS_<CHAIN>             (optional) comma-separated WS urls.
//   - FINALITY_BLOCKS_<CHAIN>    (required) finality depth.
//   - GAS_STRATEGY_<CHAIN>        (optional) per-chain override; falls back to
//     GAS_STRATEGY, then DefaultGasStrategy.
//
// Returns a ConfigLoadError when individual chains are misconfigured; the
// returned slice still contains valid entries (invalid chains are omitted).
// When CHAINS_SUPPORTED is empty, an empty slice and a nil error are
// returned.
func LoadConfigs() ([]ChainConfig, error) {
	supported := strings.TrimSpace(os.Getenv("CHAINS_SUPPORTED"))
	if supported == "" {
		return nil, nil
	}

	defaultGas := strings.TrimSpace(os.Getenv("GAS_STRATEGY"))
	if defaultGas == "" {
		defaultGas = DefaultGasStrategy
	}

	chainIDs := splitCSV(supported)
	cfgs := make([]ChainConfig, 0, len(chainIDs))
	errs := make(map[string]error)

	for _, rawID := range chainIDs {
		chainID := strings.ToLower(strings.TrimSpace(rawID))
		if chainID == "" {
			continue
		}
		upper := strings.ToUpper(chainID)

		cfg := ChainConfig{ChainID: chainID}

		// RPC urls (required).
		rpc := splitCSV(os.Getenv("RPC_URLS_" + upper))
		if len(rpc) == 0 {
			errs[chainID] = fmt.Errorf("RPC_URLS_%s is required", upper)
			continue
		}
		cfg.RPCURLs = rpc

		// WS urls (optional).
		cfg.WSURLs = splitCSV(os.Getenv("WS_URLS_" + upper))

		// Finality blocks (required).
		fbStr := strings.TrimSpace(os.Getenv("FINALITY_BLOCKS_" + upper))
		if fbStr == "" {
			errs[chainID] = fmt.Errorf("FINALITY_BLOCKS_%s is required", upper)
			continue
		}
		fb, err := strconv.ParseUint(fbStr, 10, 64)
		if err != nil {
			errs[chainID] = fmt.Errorf("FINALITY_BLOCKS_%s: %w", upper, err)
			continue
		}
		cfg.FinalityBlocks = fb

		// Gas strategy: per-chain override, else global default.
		if gs := strings.TrimSpace(os.Getenv("GAS_STRATEGY_" + upper)); gs != "" {
			cfg.GasStrategy = gs
		} else {
			cfg.GasStrategy = defaultGas
		}

		cfgs = append(cfgs, cfg)
	}

	if len(errs) > 0 {
		return cfgs, &ConfigLoadError{Errors: errs}
	}
	return cfgs, nil
}

// splitCSV splits a comma-separated env var value, trimming whitespace and
// dropping empty entries.
func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
