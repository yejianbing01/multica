package dingtalk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type noDeliveryOutboundQueries struct{}

func (noDeliveryOutboundQueries) GetChannelTaskDelivery(context.Context, pgtype.UUID) (db.ChannelTaskDelivery, error) {
	return db.ChannelTaskDelivery{}, pgx.ErrNoRows
}
func (noDeliveryOutboundQueries) GetAgentTask(context.Context, pgtype.UUID) (db.AgentTaskQueue, error) {
	panic("GetAgentTask must not run without a task delivery snapshot")
}
func (noDeliveryOutboundQueries) TaskHasChannelIngestedMessages(context.Context, pgtype.UUID) (bool, error) {
	panic("TaskHasChannelIngestedMessages must not run without a task delivery snapshot")
}
func (noDeliveryOutboundQueries) GetChannelInstallation(context.Context, db.GetChannelInstallationParams) (db.ChannelInstallation, error) {
	panic("GetChannelInstallation must not run without a task delivery snapshot")
}

func TestOutboundFailsClosedWithoutTaskDeliverySnapshot(t *testing.T) {
	o := NewOutbound(noDeliveryOutboundQueries{}, nil, nil, nil)
	event := events.Event{
		Type:          protocol.EventChatDone,
		TaskID:        "11111111-1111-1111-1111-111111111111",
		ChatSessionID: "22222222-2222-2222-2222-222222222222",
		Payload:       protocol.ChatDonePayload{Content: "must stay in Multica"},
	}
	if err := o.processEvent(context.Background(), event); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
}

type sessionReplyOutboundQueries struct {
	delivery     db.ChannelTaskDelivery
	installation db.ChannelInstallation
}

func (q sessionReplyOutboundQueries) GetChannelTaskDelivery(context.Context, pgtype.UUID) (db.ChannelTaskDelivery, error) {
	return q.delivery, nil
}

func (sessionReplyOutboundQueries) GetAgentTask(context.Context, pgtype.UUID) (db.AgentTaskQueue, error) {
	return db.AgentTaskQueue{}, nil
}

func (sessionReplyOutboundQueries) TaskHasChannelIngestedMessages(context.Context, pgtype.UUID) (bool, error) {
	return true, nil
}

func (q sessionReplyOutboundQueries) GetChannelInstallation(context.Context, db.GetChannelInstallationParams) (db.ChannelInstallation, error) {
	return q.installation, nil
}

func TestOutboundGroupReplyMentionsTriggerSenderThroughSessionWebhook(t *testing.T) {
	var (
		sessionCalls int
		gotPayload   sessionWebhookPayload
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/session" {
			t.Errorf("unexpected fallback request path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusNotFound)
			return
		}
		sessionCalls++
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &gotPayload); err != nil {
			t.Errorf("decode session reply: %v", err)
		}
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

	installationID := sessionUUID(8)
	secret := base64.StdEncoding.EncodeToString([]byte("app-secret"))
	queries := sessionReplyOutboundQueries{
		delivery: db.ChannelTaskDelivery{
			InstallationID:   installationID,
			ChannelType:      string(TypeDingTalk),
			ChannelChatID:    "group-a",
			ChatType:         "group",
			ChannelMessageID: pgtype.Text{String: "message-a", Valid: true},
			Config:           []byte(`{"conversation_type":"2","conversation_id":"group-a"}`),
		},
		installation: db.ChannelInstallation{
			ID:     installationID,
			Status: "active",
			Config: []byte(fmt.Sprintf(`{"app_id":"app-a","robot_code":"robot-a","app_secret_encrypted":%q}`, secret)),
		},
	}
	outbound := NewOutbound(queries, nil, client, nil)
	event := events.Event{
		Type:          protocol.EventChatDone,
		TaskID:        "11111111-1111-1111-1111-111111111111",
		ChatSessionID: "22222222-2222-2222-2222-222222222222",
		Payload:       protocol.ChatDonePayload{Content: "answer"},
	}
	if err := outbound.processEvent(context.Background(), event); err != nil {
		t.Fatalf("processEvent: %v", err)
	}
	if sessionCalls != 1 {
		t.Fatalf("session webhook calls = %d, want 1", sessionCalls)
	}
	if gotPayload.At == nil || len(gotPayload.At.UserIDs) != 1 || gotPayload.At.UserIDs[0] != "staff-a" {
		t.Fatalf("session reply @ = %+v", gotPayload.At)
	}
	if gotPayload.Markdown.Text != "@staff-a\n\nanswer" {
		t.Fatalf("session reply text = %q", gotPayload.Markdown.Text)
	}
}

func TestEventContent(t *testing.T) {
	cases := []struct {
		name  string
		event events.Event
		want  string
	}{
		{"chat done typed", events.Event{Type: protocol.EventChatDone, Payload: protocol.ChatDonePayload{Content: "reply"}}, "reply"},
		{"map round trip", events.Event{Type: protocol.EventChatDone, Payload: map[string]any{"content": "from map"}}, "from map"},
		{"empty map", events.Event{Type: protocol.EventChatDone, Payload: map[string]any{}}, ""},
		{"nil", events.Event{Type: protocol.EventChatDone}, ""},
		{
			"task failed with error",
			events.Event{Type: protocol.EventTaskFailed, Payload: map[string]any{"error": "task timed out", "retry_pending": false}},
			"⚠️ task timed out",
		},
		{
			// Retry-pending failures stay silent even if a mixed-version
			// publisher accidentally includes an error string.
			"task failed with retry pending",
			events.Event{Type: protocol.EventTaskFailed, Payload: map[string]any{"error": "task timed out", "failure_reason": "timeout", "retry_pending": true}},
			"",
		},
		{
			// Failure broadcasts without an error text have nothing safe to
			// deliver and stay silent.
			"task failed without error",
			events.Event{Type: protocol.EventTaskFailed, Payload: map[string]any{"failure_reason": "timeout", "retry_pending": false}},
			"",
		},
		{
			// task:failed payloads never carry "content"; it must not leak
			// through the chat-done branch.
			"task failed ignores content key",
			events.Event{Type: protocol.EventTaskFailed, Payload: map[string]any{"content": "not for delivery"}},
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := eventContent(tc.event); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
