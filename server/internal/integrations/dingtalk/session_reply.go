package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DingTalk's Stream callback includes a signed sessionWebhook that can reply to
// the originating conversation and produce a real @ notification. The regular
// /robot/groupMessages/send OpenAPI endpoint used for proactive sends cannot do
// that. The capability is temporary and must never be logged or persisted in
// plaintext, so the shared Client keeps only a message-scoped in-memory cache.

const (
	sessionWebhookHost         = "oapi.dingtalk.com"
	sessionWebhookSafetyMargin = 30 * time.Second
	maxSessionReplyContexts    = 10000
)

type sessionReplyKey struct {
	appKey    string
	messageID string
}

type sessionReplyContext struct {
	webhookURL    string
	senderStaffID string
	expiresAt     time.Time
}

type sessionWebhookAt struct {
	UserIDs []string `json:"atUserIds"`
	AtAll   bool     `json:"isAtAll"`
}

type sessionWebhookPayload struct {
	MsgType  string            `json:"msgtype"`
	Markdown markdownParam     `json:"markdown"`
	At       *sessionWebhookAt `json:"at,omitempty"`
}

type sessionWebhookResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// rememberSessionReply records the reply capability for one accepted-looking
// group callback. The shared engine remains authoritative for whether the
// message is actually ingested; unused entries disappear when their webhook
// expires and are pruned on subsequent traffic.
func (c *Client) rememberSessionReply(appKey string, data *botCallbackData) {
	if c == nil || data == nil || data.ConversationType != convTypeGroup ||
		appKey == "" || data.MsgId == "" || data.SenderStaffId == "" ||
		data.SessionWebhook == "" || data.SessionWebhookExpiredTime <= 0 {
		return
	}
	if !c.validSessionWebhookURL(data.SessionWebhook) {
		return
	}
	expiresAt := time.UnixMilli(data.SessionWebhookExpiredTime)
	cutoff := c.now().Add(sessionWebhookSafetyMargin)
	if !expiresAt.After(cutoff) {
		return
	}

	c.replyMu.Lock()
	defer c.replyMu.Unlock()
	for key, reply := range c.replyContexts {
		if !reply.expiresAt.After(cutoff) {
			delete(c.replyContexts, key)
		}
	}
	key := sessionReplyKey{appKey: appKey, messageID: data.MsgId}
	if _, exists := c.replyContexts[key]; !exists && len(c.replyContexts) >= maxSessionReplyContexts {
		// This should require an extreme number of simultaneously unfinished
		// turns. Evict the capability that expires first so the cache remains
		// strictly bounded without disturbing newer replies preferentially.
		var oldestKey sessionReplyKey
		var oldestExpiry time.Time
		for candidateKey, candidate := range c.replyContexts {
			if oldestExpiry.IsZero() || candidate.expiresAt.Before(oldestExpiry) {
				oldestKey = candidateKey
				oldestExpiry = candidate.expiresAt
			}
		}
		delete(c.replyContexts, oldestKey)
	}
	c.replyContexts[key] = sessionReplyContext{
		webhookURL:    data.SessionWebhook,
		senderStaffID: data.SenderStaffId,
		expiresAt:     expiresAt,
	}
}

// takeSessionReply returns and removes the capability for the task's exact
// trigger message. Removal makes a duplicate terminal event fall back to the
// normal send path instead of reusing the same signed reply capability.
func (c *Client) takeSessionReply(appKey, messageID string) (sessionReplyContext, bool) {
	if c == nil || appKey == "" || messageID == "" {
		return sessionReplyContext{}, false
	}
	key := sessionReplyKey{appKey: appKey, messageID: messageID}
	c.replyMu.Lock()
	reply, ok := c.replyContexts[key]
	delete(c.replyContexts, key)
	c.replyMu.Unlock()
	if !ok || !reply.expiresAt.After(c.now().Add(sessionWebhookSafetyMargin)) ||
		!c.validSessionWebhookURL(reply.webhookURL) {
		return sessionReplyContext{}, false
	}
	return reply, true
}

// sendSessionReply posts the reply through the callback-scoped webhook. It
// returns the number of chunks already delivered so callers can safely fall
// back only when no partial reply was emitted. Only the first chunk carries the
// @ notification, preventing long answers from repeatedly alerting the sender.
func (c *Client) sendSessionReply(ctx context.Context, reply sessionReplyContext, text string) (int, error) {
	if text == "" {
		return 0, nil
	}
	if reply.senderStaffID == "" || !c.validSessionWebhookURL(reply.webhookURL) {
		return 0, errors.New("dingtalk: invalid session reply context")
	}
	title := markdownTitle(text)
	sent := 0
	for index, chunk := range chunkMarkdown(text) {
		payload := sessionWebhookPayload{
			MsgType:  "markdown",
			Markdown: markdownParam{Title: title, Text: chunk},
		}
		if index == 0 {
			payload.At = &sessionWebhookAt{UserIDs: []string{reply.senderStaffID}}
		}
		if err := c.postSessionWebhook(ctx, reply.webhookURL, payload); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

func (c *Client) validSessionWebhookURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.User != nil || parsed.Host == "" || parsed.RawQuery == "" {
		return false
	}
	if c.apiBase == defaultAPIBase {
		return parsed.Scheme == "https" && strings.EqualFold(parsed.Hostname(), sessionWebhookHost)
	}
	// Tests override apiBase with an httptest server. Restrict their callback
	// webhook to that exact origin so the production SSRF boundary stays strict.
	base, err := url.Parse(c.apiBase)
	return err == nil && parsed.Scheme == base.Scheme && parsed.Host == base.Host
}

func (c *Client) postSessionWebhook(ctx context.Context, webhookURL string, payload sessionWebhookPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("dingtalk: marshal session reply: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("dingtalk: build session reply: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Do not follow a signed DingTalk URL onto a different origin. Besides the
	// SSRF boundary, keeping redirects on-origin prevents reply content and the
	// sender ID from being forwarded to an attacker-controlled endpoint.
	httpClient := *c.httpClient
	previousRedirectCheck := httpClient.CheckRedirect
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("dingtalk: too many session reply redirects")
		}
		if !c.validSessionWebhookURL(req.URL.String()) {
			return errors.New("dingtalk: refused session reply redirect")
		}
		if previousRedirectCheck != nil {
			return previousRedirectCheck(req, via)
		}
		return nil
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// net/http errors include the full request URL. Do not wrap them: the
		// session query is a signed capability and must never reach logs.
		return errors.New("dingtalk: session reply request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return fmt.Errorf("dingtalk: read session reply response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("dingtalk: session reply: http %d", resp.StatusCode)
	}
	var result sessionWebhookResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("dingtalk: decode session reply response: %w", err)
	}
	if result.ErrCode != 0 {
		return fmt.Errorf("dingtalk: session reply: code=%d message=%q", result.ErrCode, result.ErrMsg)
	}
	return nil
}
