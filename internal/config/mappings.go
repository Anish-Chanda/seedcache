package config

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

func ValidatePathMappings(mappings map[string]string) error {
	keys := make([]string, 0, len(mappings))
	for from, to := range mappings {
		for _, prefix := range []string{from, to} {
			if prefix == "" || path.IsAbs(prefix) || path.Clean(prefix) != prefix || prefix == "." || strings.ContainsRune(prefix, 0) || strings.Contains(prefix, "\\") {
				return fmt.Errorf("destination.path_mappings requires clean relative directory prefixes")
			}
			for _, part := range strings.Split(prefix, "/") {
				if part == ".." || part == ".seedcache-partial" {
					return fmt.Errorf("destination.path_mappings contains an unsafe component")
				}
			}
		}
		keys = append(keys, from)
	}
	sort.Strings(keys)
	for i, a := range keys {
		for _, b := range keys[i+1:] {
			if prefixOverlap(a, b) || prefixOverlap(mappings[a], mappings[b]) {
				return fmt.Errorf("destination.path_mappings contains overlapping source or destination prefixes")
			}
		}
	}
	return nil
}

func prefixOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
