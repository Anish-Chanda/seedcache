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
	apiPrefix        = "/api/v2"
	requestTimeout   = 10 * time.Second
	maxResponseSize  = 64 << 20
	statePollEvery   = 250 * time.Millisecond
	stateWaitLimit   = 30 * time.Second
	recheckWaitLimit = 12 * time.Hour
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
	LastActivity int64   `json:"last_activity"`
	SavePath     string  `json:"save_path"`
	DownloadPath string  `json:"download_path"`
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

// Stop pauses one torrent and waits until qBittorrent's stopped filter includes it.
func (c *Client) Stop(ctx context.Context, hash string) error {
	if err := validateTorrentHash(hash); err != nil {
		return err
	}

	opCtx, cancel := context.WithTimeout(ctx, stateWaitLimit)
	defer cancel()
	if err := c.postTorrentAction(opCtx, "stop", hash); err != nil {
		return err
	}
	return c.waitTorrentFilter(opCtx, hash, "stopped")
}

// Start resumes one torrent and waits until qBittorrent reports it in a
// seeding state. A queued upload state is accepted because queueing is a
// qBittorrent policy and the torrent is then ready to seed.
func (c *Client) Start(ctx context.Context, hash string) error {
	if err := validateTorrentHash(hash); err != nil {
		return err
	}

	opCtx, cancel := context.WithTimeout(ctx, stateWaitLimit)
	defer cancel()
	if err := c.postTorrentAction(opCtx, "start", hash); err != nil {
		return err
	}
	_, err := c.waitTorrent(opCtx, hash, func(torrent Torrent) bool {
		return isSeedingState(torrent.State)
	}, "seeding")
	return err
}

// Recheck asks qBittorrent to hash-check one stopped torrent's selected files.
// It waits for the check to start and finish, then confirms every selected
// file is complete. It never resumes the torrent.
func (c *Client) Recheck(ctx context.Context, hash string) error {
	if err := validateTorrentHash(hash); err != nil {
		return err
	}

	opCtx, cancel := context.WithTimeout(ctx, recheckWaitLimit)
	defer cancel()
	stopped, err := c.Stopped(opCtx, hash)
	if err != nil {
		return err
	}
	if !stopped {
		return fmt.Errorf("qBittorrent torrent %s must be stopped before recheck", hash)
	}

	// Do not mistake a check already in progress for the request made here.
	current, err := c.Torrent(opCtx, hash)
	if err != nil {
		return err
	}
	if isCheckingState(current.State) {
		current, err = c.waitTorrent(opCtx, hash, func(torrent Torrent) bool {
			return !isCheckingState(torrent.State)
		}, "the existing recheck to finish")
		if err != nil {
			return err
		}
	}
	checkingFilterHasTorrent, _, err := c.torrentInFilter(opCtx, hash, "checking")
	if err != nil {
		return err
	}
	// qBittorrent maps unrecognized filter names to "all". Only rely on this
	// filter when it is empty for a torrent whose direct state is idle.
	checkingFilterSupported := !checkingFilterHasTorrent
	baselineFiles, err := c.Files(opCtx, hash)
	if err != nil {
		return err
	}
	baseline, baselineIncomplete, err := selectedFileSnapshot(hash, baselineFiles)
	if err != nil {
		return err
	}

	if err := c.postTorrentAction(opCtx, "recheck", hash); err != nil {
		return err
	}
	startCtx, startCancel := context.WithTimeout(opCtx, stateWaitLimit)
	err = c.waitRecheckStart(startCtx, hash, baseline, baselineIncomplete, checkingFilterSupported)
	startCancel()
	if err != nil {
		return err
	}

	if err = c.waitRecheckFinish(opCtx, hash, checkingFilterSupported, "the requested recheck to finish"); err != nil {
		return err
	}

	torrent, err := c.Torrent(opCtx, hash)
	if err != nil {
		return err
	}
	if isMissingOrErrorState(torrent.State) {
		return fmt.Errorf("qBittorrent recheck for torrent %s ended in state %q", hash, torrent.State)
	}
	stopped, err = c.Stopped(opCtx, hash)
	if err != nil {
		return err
	}
	if !stopped {
		return fmt.Errorf("qBittorrent torrent %s is no longer stopped after recheck", hash)
	}

	files, err := c.Files(opCtx, hash)
	if err != nil {
		return err
	}
	selected := 0
	for _, file := range files {
		if file.Priority < 0 {
			return fmt.Errorf("qBittorrent recheck for torrent %s returned an invalid priority for file %q", hash, file.Name)
		}
		if file.Priority <= 0 {
			continue
		}
		selected++
		if file.Size < 0 || (file.Size > 0 && file.Progress != 1) {
			return fmt.Errorf("qBittorrent recheck for torrent %s left selected file %q incomplete", hash, file.Name)
		}
	}
	if selected == 0 {
		return fmt.Errorf("qBittorrent recheck for torrent %s found no selected files", hash)
	}
	return nil
}

// waitRecheckStart accepts either an observed checking state or a completed
// selected-file progress transition from an initially incomplete baseline.
// qBittorrent can finish small checks between API polls, so the second signal
// is useful only when the selected set moved from incomplete to complete while
// the torrent remained stopped.
func (c *Client) waitRecheckStart(ctx context.Context, hash string, baseline map[int]File, baselineIncomplete, checkingFilterSupported bool) error {
	lastState := ""
	for {
		torrent, err := c.Torrent(ctx, hash)
		if err != nil {
			return err
		}
		lastState = torrent.State
		if isCheckingState(torrent.State) {
			return nil
		}
		if checkingFilterSupported {
			checking, _, err := c.torrentInFilter(ctx, hash, "checking")
			if err != nil {
				return err
			}
			if checking {
				return nil
			}
		}
		if baselineIncomplete {
			files, err := c.Files(ctx, hash)
			if err != nil {
				return err
			}
			completed, err := selectedFilesCompletedFromBaseline(hash, baseline, files)
			if err != nil {
				return err
			}
			if completed && !isMissingOrErrorState(torrent.State) {
				stopped, err := c.Stopped(ctx, hash)
				if err != nil {
					return err
				}
				if stopped {
					return nil
				}
			}
		}

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("qBittorrent torrent %s did not reach the requested recheck to start in time (last state %q): %w", hash, lastState, ctx.Err())
			}
			return fmt.Errorf("qBittorrent torrent %s did not reach the requested recheck to start (last state %q): %w", hash, lastState, ctx.Err())
		case <-timer.C:
		}
	}
}

func (c *Client) waitRecheckFinish(ctx context.Context, hash string, checkingFilterSupported bool, expected string) error {
	ticker := time.NewTicker(statePollEvery)
	defer ticker.Stop()
	lastState := ""
	for {
		torrent, err := c.Torrent(ctx, hash)
		if err != nil {
			return err
		}
		lastState = torrent.State
		checking := isCheckingState(torrent.State)
		if !checking && checkingFilterSupported {
			checking, _, err = c.torrentInFilter(ctx, hash, "checking")
			if err != nil {
				return err
			}
		}
		if !checking {
			stopped, err := c.Stopped(ctx, hash)
			if err != nil {
				return err
			}
			if stopped {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("qBittorrent torrent %s did not reach %s in time (last state %q): %w", hash, expected, lastState, ctx.Err())
			}
			return fmt.Errorf("qBittorrent torrent %s did not reach %s (last state %q): %w", hash, expected, lastState, ctx.Err())
		case <-ticker.C:
		}
	}
}

func selectedFileSnapshot(hash string, files []File) (map[int]File, bool, error) {
	selected := make(map[int]File)
	incomplete := false
	for _, file := range files {
		if file.Priority < 0 {
			return nil, false, fmt.Errorf("qBittorrent recheck for torrent %s returned an invalid priority for file %q", hash, file.Name)
		}
		if file.Priority == 0 {
			continue
		}
		if file.Size < 0 || file.Progress < 0 || file.Progress > 1 {
			return nil, false, fmt.Errorf("qBittorrent recheck for torrent %s returned invalid size or progress for selected file %q", hash, file.Name)
		}
		selected[file.Index] = file
		if file.Size > 0 && file.Progress != 1 {
			incomplete = true
		}
	}
	if len(selected) == 0 {
		return nil, false, fmt.Errorf("qBittorrent recheck for torrent %s found no selected files", hash)
	}
	return selected, incomplete, nil
}

func selectedFilesCompletedFromBaseline(hash string, baseline map[int]File, current []File) (bool, error) {
	files := make(map[int]File, len(current))
	for _, file := range current {
		if file.Priority < 0 {
			return false, fmt.Errorf("qBittorrent recheck for torrent %s returned an invalid priority for file %q", hash, file.Name)
		}
		if file.Priority > 0 {
			files[file.Index] = file
		}
	}
	if len(files) != len(baseline) {
		return false, nil
	}
	for index, before := range baseline {
		after, ok := files[index]
		if !ok || before.Name != after.Name || before.Size != after.Size || before.Priority != after.Priority {
			return false, nil
		}
		if after.Size < 0 || after.Progress < 0 || after.Progress > 1 {
			return false, fmt.Errorf("qBittorrent recheck for torrent %s returned invalid size or progress for selected file %q", hash, after.Name)
		}
		if after.Size > 0 && after.Progress != 1 {
			return false, nil
		}
	}
	return true, nil
}

// Stopped reports qBittorrent's stopped filter for one torrent. This remains
// meaningful when the displayed state is missingFiles, which hides whether
// the torrent is stopped or running.
func (c *Client) Stopped(ctx context.Context, hash string) (bool, error) {
	if err := validateTorrentHash(hash); err != nil {
		return false, err
	}
	stopped, _, err := c.torrentInFilter(ctx, hash, "stopped")
	return stopped, err
}

func (c *Client) postTorrentAction(ctx context.Context, action, hash string) error {
	form := url.Values{"hashes": []string{hash}}
	response, err := c.authorizedRequest(ctx, http.MethodPost, "torrents/"+action, nil, form)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: torrents/%s", ErrNotFound, action)
	}
	if response.StatusCode != http.StatusOK {
		return &HTTPError{Operation: "torrents/" + action, StatusCode: response.StatusCode}
	}
	if _, err := io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20)); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("qBittorrent torrents/%s: %w", action, ctx.Err())
		}
		return fmt.Errorf("qBittorrent torrents/%s response could not be read: %w", action, err)
	}
	return nil
}

func (c *Client) waitTorrent(ctx context.Context, hash string, ready func(Torrent) bool, expected string) (Torrent, error) {
	ticker := time.NewTicker(statePollEvery)
	defer ticker.Stop()
	lastState := ""
	for {
		torrent, err := c.Torrent(ctx, hash)
		if err != nil {
			return Torrent{}, err
		}
		lastState = torrent.State
		if ready(torrent) {
			return torrent, nil
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return Torrent{}, fmt.Errorf("qBittorrent torrent %s did not reach %s in time (last state %q): %w", hash, expected, lastState, ctx.Err())
			}
			return Torrent{}, fmt.Errorf("qBittorrent torrent %s did not reach %s (last state %q): %w", hash, expected, lastState, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *Client) waitTorrentFilter(ctx context.Context, hash, filter string) error {
	ticker := time.NewTicker(statePollEvery)
	defer ticker.Stop()
	lastState := ""
	for {
		found, _, err := c.torrentInFilter(ctx, hash, filter)
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		current, err := c.Torrent(ctx, hash)
		if err != nil {
			return err
		}
		lastState = current.State
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("qBittorrent torrent %s did not enter the %s filter in time (last state %q): %w", hash, filter, lastState, ctx.Err())
			}
			return fmt.Errorf("qBittorrent torrent %s did not enter the %s filter (last state %q): %w", hash, filter, lastState, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (c *Client) torrentInFilter(ctx context.Context, hash, filter string) (bool, Torrent, error) {
	query := url.Values{"filter": []string{filter}, "hashes": []string{hash}}
	body, err := c.get(ctx, "torrents/info", query)
	if err != nil {
		return false, Torrent{}, err
	}
	var torrents []Torrent
	if err := json.Unmarshal(body, &torrents); err != nil {
		return false, Torrent{}, errors.New("qBittorrent torrents/info returned invalid JSON")
	}
	for _, torrent := range torrents {
		if strings.EqualFold(torrent.Hash, hash) {
			return true, torrent, nil
		}
	}
	return false, Torrent{}, nil
}

func isSeedingState(state string) bool {
	switch state {
	case "uploading", "stalledUP", "queuedUP", "forcedUP":
		return true
	default:
		return false
	}
}

func isCheckingState(state string) bool {
	return state == "checkingUP" || state == "checkingDL" || state == "checkingResumeData"
}

func isMissingOrErrorState(state string) bool {
	return state == "missingFiles" || state == "error"
}

func validateTorrentHash(hash string) error {
	if len(hash) != 40 && len(hash) != 64 {
		return errors.New("invalid qBittorrent torrent hash")
	}
	for _, char := range hash {
		if !(char >= '0' && char <= '9') && !(char >= 'a' && char <= 'f') && !(char >= 'A' && char <= 'F') {
			return errors.New("invalid qBittorrent torrent hash")
		}
	}
	return nil
}

func emptyJSON(body []byte) bool {
	trimmed := strings.TrimSpace(string(body))
	return trimmed == "" || trimmed == "null"
}

func (c *Client) get(ctx context.Context, operation string, query url.Values) ([]byte, error) {
	response, err := c.authorizedRequest(ctx, http.MethodGet, operation, query, nil)
	if err != nil {
		return nil, err
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

func (c *Client) authorizedRequest(ctx context.Context, method, operation string, query, form url.Values) (*http.Response, error) {
	generation, err := c.ensureAuthenticated(ctx)
	if err != nil {
		return nil, err
	}

	response, err := c.request(ctx, method, operation, query, form)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusForbidden {
		return response, nil
	}
	_ = response.Body.Close()
	if _, err := c.reauthenticate(ctx, generation); err != nil {
		return nil, err
	}
	return c.request(ctx, method, operation, query, form)
}

func (c *Client) request(ctx context.Context, method, operation string, query, form url.Values) (*http.Response, error) {
	requestURL := c.endpoint(operation, query)
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return nil, fmt.Errorf("qBittorrent %s request could not be created", operation)
	}
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
