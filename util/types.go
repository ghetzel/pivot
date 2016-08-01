package util

type Status struct {
	OK          bool   `json:"ok"`
	Application string `json:"application"`
	Version     string `json:"version"`
}
