package api

import "time"

type Health struct {
	Status string `json:"status"`
}
type Version struct {
	Version string `json:"version"`
}
type Status struct {
	StartedAt    time.Time  `json:"started_at"`
	QBittorrent  Connection `json:"qbittorrent"`
	TorrentCount int        `json:"torrent_count"`
}
type Connection struct {
	Connected      bool       `json:"connected"`
	Version        string     `json:"version,omitempty"`
	APIVersion     string     `json:"api_version,omitempty"`
	Error          string     `json:"error,omitempty"`
	LastSuccess    *time.Time `json:"last_success,omitempty"`
	AutoTMMEnabled *bool      `json:"auto_tmm_enabled,omitempty"`
}
type Torrent struct {
	Hash           string     `json:"hash"`
	Name           string     `json:"name"`
	State          string     `json:"state"`
	Progress       float64    `json:"progress"`
	TotalSize      int64      `json:"total_size"`
	Uploaded       int64      `json:"uploaded"`
	Ratio          float64    `json:"ratio"`
	SeedingSeconds int64      `json:"seeding_seconds"`
	SavePath       string     `json:"save_path"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	AutoTMM        bool       `json:"auto_tmm"`
}
type File struct {
	Index    int     `json:"index"`
	Path     string  `json:"path"`
	Size     int64   `json:"size"`
	Priority int     `json:"priority"`
	Progress float64 `json:"progress"`
}
type TorrentDetail struct {
	Torrent
	Files []File `json:"files"`
}
type TorrentList struct {
	Torrents []Torrent `json:"torrents"`
}
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type ErrorResponse struct {
	Error Error `json:"error"`
}
