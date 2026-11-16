package qbittorrent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	apiPrefix       = "/api/v2"
	requestTimeout  = 10 * time.Second
	maxResponseSize = 64 << 20
)

var ErrNotFound = errors.New("qBittorrent resource not found")

type HTTPError struct {
	Operation  string
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("qBittorrent %s returned HTTP %d", e.Operation, e.StatusCode)
}

type Client struct {
	baseURL  *url.URL
	origin   string
	username string
	password string
	http     *http.Client

	authMu         sync.Mutex
	authenticated  bool
	authGeneration uint64
	refreshFrom    uint64
	refreshErr     error
}

// New rejects URLs containing credentials, query parameters, or fragments.
func New(baseURL, username, password string) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, errors.New("invalid qBittorrent base URL")
	}

	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || !parsed.IsAbs() || parsed.Opaque != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.RawFragment != "" {
		return nil, errors.New("invalid qBittorrent base URL")
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, errors.New("could not create qBittorrent cookie jar")
	}

	return &Client{
		baseURL:  parsed,
		origin:   parsed.Scheme + "://" + parsed.Host,
		username: username,
		password: password,
		http: &http.Client{
			Jar:     jar,
			Timeout: requestTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				// In particular, never forward the login form to a redirect target.
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

type Torrent struct {
	Hash         string  `json:"hash"`
	Name         string  `json:"name"`
	State        string  `json:"state"`
	Progress     float64 `json:"progress"`
	TotalSize    int64   `json:"total_size"`
	Size         int64   `json:"size"`
	AmountLeft   int64   `json:"amount_left"`
	Uploaded     int64   `json:"uploaded"`
	Ratio        float64 `json:"ratio"`
	SeedingTime  int64   `json:"seeding_time"`
	SavePath     string  `json:"save_path"`
	ContentPath  string  `json:"content_path"`
	CompletionOn int64   `json:"completion_on"`
	AutoTMM      bool    `json:"auto_tmm"`
}

type Properties struct {
	SavePath       string  `json:"save_path"`
	CreationDate   int64   `json:"creation_date"`
	CompletionDate int64   `json:"completion_date"`
	TotalSize      int64   `json:"total_size"`
	TotalUploaded  int64   `json:"total_uploaded"`
	ShareRatio     float64 `json:"share_ratio"`
	SeedingTime    int64   `json:"seeding_time"`
}

type File struct {
	Index    int     `json:"index"`
	Name     string  `json:"name"`
	Size     int64   `json:"size"`
	Priority int     `json:"priority"`
	Progress float64 `json:"progress"`
}

type Preferences struct {
	AutoTMMEnabled *bool `json:"auto_tmm_enabled"`
}

func (c *Client) Versions(ctx context.Context) (appVersion, apiVersion string, err error) {
	appBody, err := c.get(ctx, "app/version", nil)
	if err != nil {
		return "", "", err
	}
	apiBody, err := c.get(ctx, "app/webapiVersion", nil)
	if err != nil {
		return "", "", err
	}
	return strings.TrimSpace(string(appBody)), strings.TrimSpace(string(apiBody)), nil
}

func (c *Client) Torrents(ctx context.Context) ([]Torrent, error) {
	body, err := c.get(ctx, "torrents/info", nil)
	if err != nil {
		return nil, err
	}
	var torrents []Torrent
	if err := json.Unmarshal(body, &torrents); err != nil {
		return nil, fmt.Errorf("qBittorrent torrents/info returned invalid JSON")
	}
	return torrents, nil
}

func (c *Client) Torrent(ctx context.Context, hash string) (Torrent, error) {
	if strings.TrimSpace(hash) == "" {
		return Torrent{}, fmt.Errorf("%w: empty hash", ErrNotFound)
	}
	query := url.Values{"hashes": []string{hash}}
	body, err := c.get(ctx, "torrents/info", query)
	if err != nil {
		return Torrent{}, err
	}
	var torrents []Torrent
	if err := json.Unmarshal(body, &torrents); err != nil {
		return Torrent{}, errors.New("qBittorrent torrents/info returned invalid JSON")
	}
	for _, torrent := range torrents {
		if strings.EqualFold(torrent.Hash, hash) {
			return torrent, nil
		}
	}
	return Torrent{}, fmt.Errorf("%w: torrent hash", ErrNotFound)
}

func (c *Client) Properties(ctx context.Context, hash string) (Properties, error) {
	if strings.TrimSpace(hash) == "" {
		return Properties{}, fmt.Errorf("%w: empty hash", ErrNotFound)
	}
	query := url.Values{"hash": []string{hash}}
	body, err := c.get(ctx, "torrents/properties", query)
	if err != nil {
		return Properties{}, err
	}
	if emptyJSON(body) {
		return Properties{}, fmt.Errorf("%w: torrent properties", ErrNotFound)
	}
	var properties Properties
	if err := json.Unmarshal(body, &properties); err != nil {
		return Properties{}, errors.New("qBittorrent torrents/properties returned invalid JSON")
	}
	return properties, nil
}

func (c *Client) Files(ctx context.Context, hash string) ([]File, error) {
	if strings.TrimSpace(hash) == "" {
		return nil, fmt.Errorf("%w: empty hash", ErrNotFound)
	}
	query := url.Values{"hash": []string{hash}}
	body, err := c.get(ctx, "torrents/files", query)
	if err != nil {
		return nil, err
	}
	if emptyJSON(body) {
		return nil, fmt.Errorf("%w: torrent files", ErrNotFound)
	}
	var files []File
	if err := json.Unmarshal(body, &files); err != nil {
		return nil, errors.New("qBittorrent torrents/files returned invalid JSON")
	}
	return files, nil
}

func (c *Client) Preferences(ctx context.Context) (Preferences, error) {
	body, err := c.get(ctx, "app/preferences", nil)
	if err != nil {
		return Preferences{}, err
	}
	var preferences Preferences
	if err := json.Unmarshal(body, &preferences); err != nil {
		return Preferences{}, errors.New("qBittorrent app/preferences returned invalid JSON")
	}
	return preferences, nil
}

func emptyJSON(body []byte) bool {
	trimmed := strings.TrimSpace(string(body))
	return trimmed == "" || trimmed == "null"
}

func (c *Client) get(ctx context.Context, operation string, query url.Values) ([]byte, error) {
	generation, err := c.ensureAuthenticated(ctx)
	if err != nil {
		return nil, err
	}

	response, err := c.request(ctx, operation, query)
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusForbidden {
		_ = response.Body.Close()
		_, err = c.reauthenticate(ctx, generation)
		if err != nil {
			return nil, err
		}
		response, err = c.request(ctx, operation, query)
		if err != nil {
			return nil, err
		}
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, operation)
	}
	if response.StatusCode != http.StatusOK {
		return nil, &HTTPError{Operation: operation, StatusCode: response.StatusCode}
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseSize+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("qBittorrent %s: %w", operation, ctx.Err())
		}
		return nil, fmt.Errorf("qBittorrent %s response could not be read: %w", operation, err)
	}
	if len(body) > maxResponseSize {
		return nil, fmt.Errorf("qBittorrent %s response exceeded the size limit", operation)
	}
	return body, nil
}

func (c *Client) request(ctx context.Context, operation string, query url.Values) (*http.Response, error) {
	requestURL := c.endpoint(operation, query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("qBittorrent %s request could not be created", operation)
	}
	req.Header.Set("Referer", c.origin)
	req.Header.Set("Origin", c.origin)
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("qBittorrent %s: %w", operation, ctx.Err())
		}
		return nil, requestFailure(operation, err)
	}
	return resp, nil
}

func (c *Client) endpoint(operation string, query url.Values) string {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(c.baseURL.Path, "/") + apiPrefix + "/" + operation
	endpoint.RawPath = ""
	endpoint.RawQuery = ""
	endpoint.ForceQuery = false
	if len(query) > 0 {
		endpoint.RawQuery = query.Encode()
	}
	return endpoint.String()
}

func (c *Client) ensureAuthenticated(ctx context.Context) (uint64, error) {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	if c.authenticated {
		return c.authGeneration, nil
	}
	if err := c.login(ctx); err != nil {
		return 0, err
	}
	c.authenticated = true
	c.authGeneration++
	return c.authGeneration, nil
}

// Generation changes let concurrent 403 responses share one SID refresh.
func (c *Client) reauthenticate(ctx context.Context, failedGeneration uint64) (uint64, error) {
	c.authMu.Lock()
	defer c.authMu.Unlock()
	if c.authGeneration != failedGeneration {
		if c.authenticated {
			return c.authGeneration, nil
		}
		if c.refreshFrom == failedGeneration && c.refreshErr != nil {
			return 0, c.refreshErr
		}
	}
	c.authenticated = false
	c.authGeneration++
	c.refreshFrom = failedGeneration
	if err := c.login(ctx); err != nil {
		c.refreshErr = err
		return 0, err
	}
	c.authenticated = true
	c.refreshErr = nil
	return c.authGeneration, nil
}

func (c *Client) login(ctx context.Context) error {
	form := url.Values{
		"username": []string{c.username},
		"password": []string{c.password},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("auth/login", nil), strings.NewReader(form.Encode()))
	if err != nil {
		return errors.New("qBittorrent login request could not be created")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", c.origin)
	req.Header.Set("Origin", c.origin)
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("qBittorrent login: %w", ctx.Err())
		}
		return requestFailure("auth/login", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &HTTPError{Operation: "auth/login", StatusCode: resp.StatusCode}
	}
	// Drain the small response for connection reuse without retaining or
	// reflecting server-provided text in an error.
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)); err != nil {
		return fmt.Errorf("qBittorrent auth/login response could not be read: %w", err)
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "SID" && cookie.Value != "" {
			return nil
		}
	}
	return errors.New("qBittorrent authentication failed")
}

func requestFailure(operation string, err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return fmt.Errorf("qBittorrent %s request failed: %w", operation, urlErr.Err)
	}
	return fmt.Errorf("qBittorrent %s request failed", operation)
}
