package config

import "testing"

func TestValidatePathMappings(t *testing.T) {
	for _, m := range []map[string]string{nil, {}, {"movies": "Movies", "tv": "TV Shows"}, {"media/电影": "影片"}} {
		if err := ValidatePathMappings(m); err != nil {
			t.Fatalf("valid mappings %v: %v", m, err)
		}
	}
	for _, m := range []map[string]string{
		{"": "Movies"}, {"movies": ""}, {"/movies": "Movies"}, {"movies": "/Movies"}, {"movies": "../Movies"},
		{"../movies": "Movies"}, {"movies/../tv": "Movies"}, {"movies": "Movies//Nested"}, {"movies": ".seedcache-partial"},
		{"movies": "Movies", "movies/child": "Other"}, {"movies": "Movies", "tv": "Movies"}, {"movies": "Library", "tv": "Library/TV"},
		{"movies": "Movies\\TV"}, {"movies": "Movies\x00bad"},
	} {
		if err := ValidatePathMappings(m); err == nil {
			t.Errorf("accepted unsafe mappings %v", m)
		}
	}
}
