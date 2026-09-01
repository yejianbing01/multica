package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type sessionReplyRoundTripper func(*http.Request) (*http.Response, error)

func (f sessionReplyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestClientSessionReplyContextIsMessageScoped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	client := NewClient(server.Client(), server.URL)
	now := time.Unix(1_700_000_000, 0)
	client.now = func() time.Time { return now }
	client.rememberSessionReply("app-a", &botCallbackData{
		ConversationType:          convTypeGroup,
		MsgId:                     "message-a",
		SenderStaffId:             "staff-a",
		SessionWebhook:            server.URL + "/session?token=secret",
		SessionWebhookExpiredTime: now.Add(time.Hour).UnixMilli(),
	})
	client.rememberSessionReply("app-a", &botCallbackData{
		ConversationType:          convTypeGroup,
		MsgId:                     "message-b",
		SenderStaffId:             "staff-b",
		SessionWebhook:            server.URL + "/session?token=secret-b",
		SessionWebhookExpiredTime: now.Add(time.Hour).UnixMilli(),
	})

	first, ok := client.takeSessionReply("app-a", "message-a")
	if !ok || first.senderStaffID != "staff-a" {
		t.Fatalf("message-a reply context = %+v, ok=%v", first, ok)
	}
	second, ok := client.takeSessionReply("app-a", "message-b")
	if !ok || second.senderStaffID != "staff-b" {
		t.Fatalf("message-b reply context = %+v, ok=%v", second, ok)
	}
	if _, ok := client.takeSessionReply("app-a", "message-a"); ok {
		t.Fatal("a consumed reply context must not be reusable")
	}
}

func TestClientSessionReplyRejectsExpiredAndUntrustedWebhooks(t *testing.T) {
	client := NewClient(nil, "")
	now := time.Unix(1_700_000_000, 0)
	client.now = func() time.Time { return now }
	base := botCallbackData{
		ConversationType:          convTypeGroup,
		MsgId:                     "message-a",
		SenderStaffId:             "staff-a",
		SessionWebhook:            "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
		SessionWebhookExpiredTime: now.Add(time.Hour).UnixMilli(),
	}

	expired := base
	expired.SessionWebhookExpiredTime = now.Add(10 * time.Second).UnixMilli()
	client.rememberSessionReply("app-a", &expired)
	if _, ok := client.takeSessionReply("app-a", "message-a"); ok {
		t.Fatal("an effectively expired webhook must not be cached")
	}

	untrusted := base
	untrusted.MsgId = "message-b"
	untrusted.SessionWebhook = "https://example.com/collect?token=secret"
	client.rememberSessionReply("app-a", &untrusted)
	if _, ok := client.takeSessionReply("app-a", "message-b"); ok {
		t.Fatal("a non-DingTalk webhook must not cross the outbound SSRF boundary")
	}
}

func TestClientSessionReplyMentionsSenderOnlyOnFirstChunk(t *testing.T) {
	var (
		mu       sync.Mutex
		payloads []sessionWebhookPayload
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload sessionWebhookPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		mu.Lock()
		payloads = append(payloads, payload)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer server.Close()

	client := NewClient(server.Client(), server.URL)
	sent, err := client.sendSessionReply(context.Background(), sessionReplyContext{
		webhookURL:    server.URL + "/session?token=secret",
		senderStaffID: "staff-a",
		expiresAt:     time.Now().Add(time.Hour),
	}, strings.Repeat("x", markdownByteBudget+100))
	if err != nil {
		t.Fatalf("sendSessionReply: %v", err)
	}
	if sent != 2 || len(payloads) != 2 {
		t.Fatalf("sent/payloads = %d/%d, want 2/2", sent, len(payloads))
	}
	if payloads[0].At == nil || len(payloads[0].At.UserIDs) != 1 || payloads[0].At.UserIDs[0] != "staff-a" {
		t.Fatalf("first chunk @ = %+v", payloads[0].At)
	}
	if payloads[1].At != nil {
		t.Fatalf("continuation chunk must not repeat @: %+v", payloads[1].At)
	}
	if payloads[0].MsgType != "markdown" || payloads[0].Markdown.Title == "" {
		t.Fatalf("first payload = %+v", payloads[0])
	}
}

func TestClientSessionReplyDoesNotExposeSignedURLInErrors(t *testing.T) {
	httpClient := &http.Client{Transport: sessionReplyRoundTripper(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed for " + req.URL.String())
	})}
	client := NewClient(httpClient, "https://session.test")
	_, err := client.sendSessionReply(context.Background(), sessionReplyContext{
		webhookURL:    "https://session.test/reply?token=do-not-log",
		senderStaffID: "staff-a",
		expiresAt:     time.Now().Add(time.Hour),
	}, "answer")
	if err == nil {
		t.Fatal("expected request failure")
	}
	if strings.Contains(err.Error(), "do-not-log") || strings.Contains(err.Error(), "session.test") {
		t.Fatalf("session reply error exposed signed URL: %q", err)
	}
}
