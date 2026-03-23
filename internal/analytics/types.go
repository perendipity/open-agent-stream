package analytics

import "time"

const (
	DatasetVersion    = "analytics-dataset/v1"
	DerivationVersion = "1"
)

type CoverageMode string

const (
	CoverageCompleteHistory       CoverageMode = "complete_history"
	CoverageCachePreservedHistory CoverageMode = "cache_preserved_history"
	CoverageRetainedWindowOnly    CoverageMode = "retained_window_only"
)

type MachineIDOrigin string

const (
	MachineIDOriginCaptured       MachineIDOrigin = "captured"
	MachineIDOriginConfigBackfill MachineIDOrigin = "config_backfill"
)

type BuildState string

const (
	BuildStateEmpty    BuildState = "empty"
	BuildStateBuilding BuildState = "building"
	BuildStateReady    BuildState = "ready"
)

type OffsetRange struct {
	Min int64 `json:"min,omitempty"`
	Max int64 `json:"max,omitempty"`
}

type Status struct {
	Path                    string           `json:"path"`
	Exists                  bool             `json:"exists"`
	SizeBytes               int64            `json:"size_bytes,omitempty"`
	DatasetVersion          string           `json:"dataset_version,omitempty"`
	DerivationVersion       string           `json:"derivation_version,omitempty"`
	EngineVersion           string           `json:"engine_version,omitempty"`
	MachineID               string           `json:"machine_id,omitempty"`
	MachineIDOrigin         string           `json:"machine_id_origin,omitempty"`
	LedgerInstanceID        string           `json:"ledger_instance_id,omitempty"`
	BuildState              string           `json:"build_state,omitempty"`
	LastCleanOffset         int64            `json:"last_clean_offset,omitempty"`
	BuildStartedAt          *time.Time       `json:"build_started_at,omitempty"`
	BuildCompletedAt        *time.Time       `json:"build_completed_at,omitempty"`
	CacheCoveredOffsetRange *OffsetRange     `json:"cache_covered_offset_range,omitempty"`
	RetainedOffsetRange     *OffsetRange     `json:"retained_offset_range,omitempty"`
	CoverageMode            string           `json:"coverage_mode,omitempty"`
	RowCounts               map[string]int64 `json:"row_counts,omitempty"`
}

type BuildOptions struct {
	Path    string
	Rebuild bool
}

type QueryOptions struct {
	Path    string
	SQL     string
	Preset  string
	Rebuild bool
}

type ExportOptions struct {
	Path      string
	OutputDir string
	Rebuild   bool
}

type Manifest struct {
	SnapshotID                  string          `json:"snapshot_id"`
	DatasetVersion              string          `json:"dataset_version"`
	DerivationVersion           string          `json:"derivation_version"`
	EngineVersion               string          `json:"engine_version"`
	MachineID                   string          `json:"machine_id"`
	MachineIDOrigin             string          `json:"machine_id_origin"`
	LedgerInstanceID            string          `json:"ledger_instance_id"`
	ExportedAt                  time.Time       `json:"exported_at"`
	CoverageMode                string          `json:"coverage_mode"`
	CacheCoveredOffsetRange     *OffsetRange    `json:"cache_covered_offset_range,omitempty"`
	RetainedOffsetRangeAtExport *OffsetRange    `json:"retained_offset_range_at_export,omitempty"`
	Tables                      []ManifestTable `json:"tables"`
}

type ManifestTable struct {
	Name       string   `json:"name"`
	Rows       int64    `json:"rows"`
	File       string   `json:"file"`
	SHA256     string   `json:"sha256"`
	PrimaryKey []string `json:"primary_key,omitempty"`
}
