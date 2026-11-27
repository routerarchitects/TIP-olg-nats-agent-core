package agentcore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/internal/registry"
	"github.com/nats-io/nats.go"
)

func validTestStateEnvelope() StateEnvelope {
	return StateEnvelope{
		Version:   "1.0",
		Target:    "vyos",
		UUID:      "1793880893",
		Payload:   json.RawMessage(`{"version":1,"unit":{"uptime":100},"interfaces":[]}`),
		Timestamp: time.Unix(1700000000, 0).UTC(),
	}
}

func TestStateEnvelope_JSONRoundTrip(t *testing.T) {
	want := validTestStateEnvelope()
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var got StateEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if got.Version != want.Version {
		t.Fatalf("expected Version %q, got %q", want.Version, got.Version)
	}
	if got.Target != want.Target {
		t.Fatalf("expected Target %q, got %q", want.Target, got.Target)
	}
	if got.UUID != want.UUID {
		t.Fatalf("expected UUID %q, got %q", want.UUID, got.UUID)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Fatalf("expected Payload %s, got %s", string(want.Payload), string(got.Payload))
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("expected Timestamp %v, got %v", want.Timestamp, got.Timestamp)
	}
}

func TestStateGetReply_JSONRoundTrip(t *testing.T) {
	// Success path
	env := validTestStateEnvelope()
	successReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		State:   &env,
	}
	data, err := json.Marshal(successReply)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var gotSuccess StateGetReply
	if err := json.Unmarshal(data, &gotSuccess); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if gotSuccess.State == nil || gotSuccess.Error != nil {
		t.Fatalf("expected State present and Error nil, got %+v", gotSuccess)
	}
	if gotSuccess.State.Target != "vyos" {
		t.Fatalf("expected State.Target %q, got %q", "vyos", gotSuccess.State.Target)
	}

	// Error path
	errReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		Error: &StateGetWireError{
			Code:    CodeServiceBusy,
			Message: "All state collection workers are occupied",
		},
	}
	data, err = json.Marshal(errReply)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var gotErr StateGetReply
	if err := json.Unmarshal(data, &gotErr); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if gotErr.Error == nil || gotErr.State != nil {
		t.Fatalf("expected Error present and State nil, got %+v", gotErr)
	}
	if gotErr.Error.Code != CodeServiceBusy {
		t.Fatalf("expected Code %q, got %q", CodeServiceBusy, gotErr.Error.Code)
	}
}

func TestValidateStateEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*StateEnvelope)
		wantErr bool
		msgPart string
	}{
		{
			name:    "valid envelope",
			mutate:  func(e *StateEnvelope) {},
			wantErr: false,
		},
		{
			name: "missing version",
			mutate: func(e *StateEnvelope) {
				e.Version = ""
			},
			wantErr: true,
			msgPart: "version is required",
		},
		{
			name: "invalid version",
			mutate: func(e *StateEnvelope) {
				e.Version = "2.0"
			},
			wantErr: true,
			msgPart: "version must be 1.0",
		},
		{
			name: "missing target",
			mutate: func(e *StateEnvelope) {
				e.Target = ""
			},
			wantErr: true,
			msgPart: "target is required",
		},
		{
			name: "invalid target with space",
			mutate: func(e *StateEnvelope) {
				e.Target = "vy os"
			},
			wantErr: true,
			msgPart: "target cannot contain whitespace",
		},
		{
			name: "missing timestamp",
			mutate: func(e *StateEnvelope) {
				e.Timestamp = time.Time{}
			},
			wantErr: true,
			msgPart: "timestamp is required",
		},
		{
			name: "empty payload",
			mutate: func(e *StateEnvelope) {
				e.Payload = nil
			},
			wantErr: true,
			msgPart: "payload is required",
		},
		{
			name: "invalid JSON payload",
			mutate: func(e *StateEnvelope) {
				e.Payload = json.RawMessage(`{invalid`)
			},
			wantErr: true,
			msgPart: "must contain valid JSON",
		},
		{
			name: "oversized payload",
			mutate: func(e *StateEnvelope) {
				// > 1MB payload
				big := make([]byte, maxStatePayloadSize+10)
				for i := range big {
					big[i] = ' '
				}
				big[0] = '{'
				big[len(big)-1] = '}'
				e.Payload = json.RawMessage(big)
			},
			wantErr: true,
			msgPart: "payload exceeds",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := validTestStateEnvelope()
			tc.mutate(&env)
			err := validateStateEnvelope("test_op", env)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.msgPart != "" && !strings.Contains(err.Error(), tc.msgPart) {
					t.Fatalf("expected error containing %q, got %q", tc.msgPart, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
			}
		})
	}
}

func TestPublishState_Success(t *testing.T) {
	client, pub := newSendTestClient(t)
	env := validTestStateEnvelope()

	err := client.PublishState(context.Background(), env)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if len(pub.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(pub.calls))
	}
	call := pub.calls[0]
	if call.op != "publish_state" {
		t.Fatalf("expected op %q, got %q", "publish_state", call.op)
	}
	if call.kind != string(registry.KindState) {
		t.Fatalf("expected kind %q, got %q", registry.KindState, call.kind)
	}
	if call.subject != "state.vyos" {
		t.Fatalf("expected subject %q, got %q", "state.vyos", call.subject)
	}

	var decoded StateEnvelope
	if err := json.Unmarshal(call.payload, &decoded); err != nil {
		t.Fatalf("failed to unmarshal published payload: %v", err)
	}
	if decoded.Target != "vyos" || decoded.UUID != "1793880893" {
		t.Fatalf("published payload mismatch: %+v", decoded)
	}
}

func TestPublishState_ValidationErrors(t *testing.T) {
	client, pub := newSendTestClient(t)

	// Missing version
	env := validTestStateEnvelope()
	env.Version = ""
	err := client.PublishState(context.Background(), env)
	requireErrorCode(t, err, CodeValidation)

	// Cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = client.PublishState(ctx, validTestStateEnvelope())
	requireErrorCode(t, err, CodeValidation)

	if len(pub.calls) != 0 {
		t.Fatalf("expected 0 publish calls on validation failure, got %d", len(pub.calls))
	}
}

func TestRegisterStateHandler_RegistrationAndHealth(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	err = client.RegisterStateHandler("vyos", func(context.Context, StateEnvelope) error { return nil }, WithQueueGroup("state-workers"))
	if err != nil {
		t.Fatalf("expected nil registration error, got %v", err)
	}

	if client.Health().RegisteredSubscriptions != 1 {
		t.Fatalf("expected 1 registered subscription, got %d", client.Health().RegisteredSubscriptions)
	}

	snapshots := client.subscriptions.List()
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 subscription snapshot, got %d", len(snapshots))
	}
	snap := snapshots[0]
	if snap.Kind != registry.KindState {
		t.Fatalf("expected kind %q, got %q", registry.KindState, snap.Kind)
	}
	if snap.Subject != "state.vyos" {
		t.Fatalf("expected subject %q, got %q", "state.vyos", snap.Subject)
	}
	if snap.QueueGroup != "state-workers" {
		t.Fatalf("expected queue group %q, got %q", "state-workers", snap.QueueGroup)
	}
}

func TestRegisterStateHandler_CallbackInvocation(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	var received StateEnvelope
	calls := 0
	cb := client.bindStateCallback(func(_ context.Context, env StateEnvelope) error {
		calls++
		received = env
		return nil
	})

	// 1. Valid payload
	validEnv := validTestStateEnvelope()
	data, _ := json.Marshal(validEnv)
	cb(&nats.Msg{Subject: "state.vyos", Data: data})

	if calls != 1 {
		t.Fatalf("expected 1 callback call, got %d", calls)
	}
	if received.Target != "vyos" || received.UUID != "1793880893" {
		t.Fatalf("received state mismatch: %+v", received)
	}

	// 2. Oversized payload (> 1MB) dropped without invoking handler
	calls = 0
	oversized := make([]byte, maxStatePayloadSize+100)
	cb(&nats.Msg{Subject: "state.vyos", Data: oversized})
	if calls != 0 {
		t.Fatalf("expected oversized payload to be dropped, got %d calls", calls)
	}

	// 3. Malformed JSON dropped without invoking handler
	calls = 0
	cb(&nats.Msg{Subject: "state.vyos", Data: []byte("{invalid-json")})
	if calls != 0 {
		t.Fatalf("expected malformed JSON to be dropped, got %d calls", calls)
	}

	// 4. Nil message dropped safely
	cb(nil)

	// 5. Handler panic recovered safely
	panicCB := client.bindStateCallback(func(_ context.Context, env StateEnvelope) error {
		panic("boom")
	})
	panicCB(&nats.Msg{Subject: "state.vyos", Data: data})
}

func TestRegisterStateGetHandler_Registration(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	err = client.RegisterStateGetHandler("vyos", func(context.Context) (StateEnvelope, error) {
		return validTestStateEnvelope(), nil
	})
	if err != nil {
		t.Fatalf("expected nil registration error, got %v", err)
	}

	if client.Health().RegisteredSubscriptions != 1 {
		t.Fatalf("expected 1 registered subscription, got %d", client.Health().RegisteredSubscriptions)
	}

	snapshots := client.subscriptions.List()
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 subscription snapshot, got %d", len(snapshots))
	}
	snap := snapshots[0]
	if snap.Kind != registry.KindStateGet {
		t.Fatalf("expected kind %q, got %q", registry.KindStateGet, snap.Kind)
	}
	if snap.Subject != "state.get.vyos" {
		t.Fatalf("expected subject %q, got %q", "state.get.vyos", snap.Subject)
	}
}

func TestDecodeStateGetReply(t *testing.T) {
	// 1. Success reply
	env := validTestStateEnvelope()
	successReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		State:   &env,
	}
	data, _ := json.Marshal(successReply)
	got, err := decodeStateGetReply(data)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got.UUID != "1793880893" {
		t.Fatalf("expected UUID %q, got %q", "1793880893", got.UUID)
	}

	// 2. ServiceBusy error reply
	busyReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		Error: &StateGetWireError{
			Code:    CodeServiceBusy,
			Message: "All state collection workers are occupied",
		},
	}
	data, _ = json.Marshal(busyReply)
	_, err = decodeStateGetReply(data)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var stateErr *StateGetError
	if !errors.As(err, &stateErr) {
		t.Fatalf("expected *StateGetError, got %T: %v", err, err)
	}
	if stateErr.Code != CodeServiceBusy {
		t.Fatalf("expected code %q, got %q", CodeServiceBusy, stateErr.Code)
	}

	// 3. CollectionTimeout error reply
	timeoutReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		Error: &StateGetWireError{
			Code:    CodeCollectionTimeout,
			Message: "state collection timed out",
		},
	}
	data, _ = json.Marshal(timeoutReply)
	_, err = decodeStateGetReply(data)
	if !errors.As(err, &stateErr) || stateErr.Code != CodeCollectionTimeout {
		t.Fatalf("expected CodeCollectionTimeout, got %v", err)
	}

	// 4. CollectionFailed error reply
	failedReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		Error: &StateGetWireError{
			Code:    CodeCollectionFailed,
			Message: "kernel counter collection failed",
		},
	}
	data, _ = json.Marshal(failedReply)
	_, err = decodeStateGetReply(data)
	if !errors.As(err, &stateErr) || stateErr.Code != CodeCollectionFailed {
		t.Fatalf("expected CodeCollectionFailed, got %v", err)
	}

	// 5. Malformed: Dual state and error
	dualReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		State:   &env,
		Error: &StateGetWireError{
			Code:    CodeCollectionFailed,
			Message: "dual failure",
		},
	}
	data, _ = json.Marshal(dualReply)
	_, err = decodeStateGetReply(data)
	if !errors.Is(err, ErrMalformedStateGetReply) {
		t.Fatalf("expected ErrMalformedStateGetReply for dual reply, got %v", err)
	}

	// 6. Malformed: Neither state nor error
	emptyReply := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
	}
	data, _ = json.Marshal(emptyReply)
	_, err = decodeStateGetReply(data)
	if !errors.Is(err, ErrMalformedStateGetReply) {
		t.Fatalf("expected ErrMalformedStateGetReply for empty reply, got %v", err)
	}

	// 7. Malformed: Empty raw data
	_, err = decodeStateGetReply([]byte{})
	if !errors.Is(err, ErrMalformedStateGetReply) {
		t.Fatalf("expected ErrMalformedStateGetReply for empty data, got %v", err)
	}

	// 8. Malformed: Invalid JSON
	_, err = decodeStateGetReply([]byte(`{invalid`))
	if !errors.Is(err, ErrMalformedStateGetReply) {
		t.Fatalf("expected ErrMalformedStateGetReply for invalid JSON, got %v", err)
	}

	// 9. Malformed: Invalid version
	wrongVerReply := StateGetReply{
		Version: "2.0",
		Target:  "vyos",
		State:   &env,
	}
	data, _ = json.Marshal(wrongVerReply)
	_, err = decodeStateGetReply(data)
	if !errors.Is(err, ErrMalformedStateGetReply) {
		t.Fatalf("expected ErrMalformedStateGetReply for wrong version, got %v", err)
	}
}

func TestRequestState_DelegationAndErrors(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	// 1. Success via requestStateFn
	client.requestStateFn = func(_ context.Context, target string) (StateEnvelope, error) {
		env := validTestStateEnvelope()
		env.Target = target
		return env, nil
	}
	got, err := client.RequestState(context.Background(), "vyos")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got.Target != "vyos" {
		t.Fatalf("expected target vyos, got %q", got.Target)
	}

	// 2. Error propagation via requestStateFn
	client.requestStateFn = func(_ context.Context, _ string) (StateEnvelope, error) {
		return StateEnvelope{}, &StateGetError{Code: CodeServiceBusy, Message: "busy"}
	}
	_, err = client.RequestState(context.Background(), "vyos")
	var stateErr *StateGetError
	if !errors.As(err, &stateErr) || stateErr.Code != CodeServiceBusy {
		t.Fatalf("expected CodeServiceBusy, got %v", err)
	}

	// 3. Validation: invalid target
	client.requestStateFn = nil
	_, err = client.RequestState(context.Background(), "invalid target")
	requireErrorCode(t, err, CodeValidation)

	// 4. Validation: cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.RequestState(ctx, "vyos")
	requireErrorCode(t, err, CodeValidation)
}

func TestRegisterStateGetHandler_Saturation_ServiceBusy(t *testing.T) {
	// Worker pool capacity is 5.
	cfg := testConfig()
	cfg.StateGetWorkers = 5
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	// Fill all 5 worker slots
	for i := 0; i < 5; i++ {
		client.stateGetSem <- struct{}{}
	}

	// The 6th request should immediately receive CodeServiceBusy
	cb := client.bindStateGetCallback("vyos", func(context.Context) (StateEnvelope, error) {
		return validTestStateEnvelope(), nil
	})

	// When saturated, the callback attempts to publish to reply subject.
	// Since there's no real NATS session connected, it returns immediately without blocking.
	cb(&nats.Msg{
		Subject: "state.get.vyos",
		Reply:   "_INBOX.test1234",
		Data:    nil,
	})

	// Verify semaphore is still at 5 (all 5 filled)
	if len(client.stateGetSem) != 5 {
		t.Fatalf("expected sem length 5, got %d", len(client.stateGetSem))
	}

	// Drain semaphore
	for i := 0; i < 5; i++ {
		<-client.stateGetSem
	}

	// Verify semaphore is now 0 (all released)
	if len(client.stateGetSem) != 0 {
		t.Fatalf("expected sem length 0, got %d", len(client.stateGetSem))
	}
}

func TestRegisterStateGetHandler_CapacityRelease_OnSuccess_OnError_OnPanic(t *testing.T) {
	cfg := testConfig()
	cfg.StateGetWorkers = 2
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	// 1. Success release
	var wg sync.WaitGroup
	wg.Add(1)
	cb := client.bindStateGetCallback("vyos", func(context.Context) (StateEnvelope, error) {
		defer wg.Done()
		return validTestStateEnvelope(), nil
	})
	cb(&nats.Msg{Subject: "state.get.vyos", Reply: "_INBOX.test", Data: nil})
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
	if len(client.stateGetSem) != 0 {
		t.Fatalf("expected sem length 0 after success, got %d", len(client.stateGetSem))
	}

	// 2. Error release
	wg.Add(1)
	cbErr := client.bindStateGetCallback("vyos", func(context.Context) (StateEnvelope, error) {
		defer wg.Done()
		return StateEnvelope{}, errors.New("collection failed")
	})
	cbErr(&nats.Msg{Subject: "state.get.vyos", Reply: "_INBOX.test", Data: nil})
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
	if len(client.stateGetSem) != 0 {
		t.Fatalf("expected sem length 0 after error, got %d", len(client.stateGetSem))
	}

	// 3. Panic release
	wg.Add(1)
	cbPanic := client.bindStateGetCallback("vyos", func(context.Context) (StateEnvelope, error) {
		defer wg.Done()
		panic("crash")
	})
	cbPanic(&nats.Msg{Subject: "state.get.vyos", Reply: "_INBOX.test", Data: nil})
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
	if len(client.stateGetSem) != 0 {
		t.Fatalf("expected sem length 0 after panic, got %d", len(client.stateGetSem))
	}
}

func TestRegisterStateGetHandler_Timeout_PromptsReplyAndRetainsSlot(t *testing.T) {
	cfg := testConfig()
	cfg.StateGetWorkers = 2
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)
	bg := context.Background()
	client.handlerCtx.Store(&bg)
	client.stateGetTimeout = 40 * time.Millisecond

	replyCh := make(chan []byte, 1)
	client.publishStateReply = func(_ string, data []byte) {
		replyCh <- data
	}

	release := make(chan struct{})
	done := make(chan struct{})

	cb := client.bindStateGetCallback("vyos", func(_ context.Context) (StateEnvelope, error) {
		defer close(done)
		// Deliberately ignore ctx.Done() to simulate a stuck/blocking system call
		<-release
		return validTestStateEnvelope(), nil
	})

	cb(&nats.Msg{Subject: "state.get.vyos", Reply: "_INBOX.timeout", Data: nil})

	// 1. Timeout reply must be published promptly (~40ms) even while handler is still blocked
	select {
	case data := <-replyCh:
		var reply StateGetReply
		if err := json.Unmarshal(data, &reply); err != nil {
			t.Fatalf("unmarshal timeout reply: %v", err)
		}
		if reply.Error == nil || reply.Error.Code != CodeCollectionTimeout {
			t.Fatalf("expected CodeCollectionTimeout, got %+v", reply.Error)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected prompt timeout reply before blocked handler returned")
	}

	// 2. Worker slot must remain occupied while the underlying handler is still running
	if len(client.stateGetSem) != 1 {
		t.Fatalf("expected worker slot retained (len=1) while handler still blocked, got %d", len(client.stateGetSem))
	}

	// 3. Unblock handler and verify slot is released cleanly
	close(release)
	<-done
	time.Sleep(20 * time.Millisecond)
	if len(client.stateGetSem) != 0 {
		t.Fatalf("expected worker slot released (len=0) after handler exit, got %d", len(client.stateGetSem))
	}
}

func TestRegisterStateGetHandler_InvalidEnvelope_ReturnsCollectionFailed(t *testing.T) {
	cases := []struct {
		name string
		env  StateEnvelope
	}{
		{
			name: "invalid_version",
			env: StateEnvelope{
				Version:   "2.0",
				Target:    "vyos",
				Timestamp: time.Now().UTC(),
				Payload:   json.RawMessage(`{"ok":true}`),
			},
		},
		{
			name: "mismatched_target",
			env: StateEnvelope{
				Version:   "1.0",
				Target:    "other",
				Timestamp: time.Now().UTC(),
				Payload:   json.RawMessage(`{"ok":true}`),
			},
		},
		{
			name: "missing_payload",
			env: StateEnvelope{
				Version:   "1.0",
				Target:    "vyos",
				Timestamp: time.Now().UTC(),
			},
		},
		{
			name: "oversized_payload",
			env: StateEnvelope{
				Version:   "1.0",
				Target:    "vyos",
				Timestamp: time.Now().UTC(),
				Payload:   json.RawMessage(`"` + strings.Repeat("a", maxStatePayloadSize+1) + `"`),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.StateGetWorkers = 2
			client, err := New(cfg)
			if err != nil {
				t.Fatalf("New returned unexpected error: %v", err)
			}
			client.callbacksEnabled.Store(true)
			bg := context.Background()
			client.handlerCtx.Store(&bg)

			replyCh := make(chan []byte, 1)
			client.publishStateReply = func(_ string, data []byte) {
				replyCh <- data
			}

			cb := client.bindStateGetCallback("vyos", func(_ context.Context) (StateEnvelope, error) {
				return tc.env, nil
			})
			cb(&nats.Msg{Subject: "state.get.vyos", Reply: "_INBOX.invalid", Data: nil})

			select {
			case data := <-replyCh:
				var reply StateGetReply
				if err := json.Unmarshal(data, &reply); err != nil {
					t.Fatalf("unmarshal reply: %v", err)
				}
				if reply.State != nil {
					t.Fatalf("expected nil State on invalid envelope, got %+v", reply.State)
				}
				if reply.Error == nil || reply.Error.Code != CodeCollectionFailed {
					t.Fatalf("expected CodeCollectionFailed, got %+v", reply.Error)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("timed out waiting for state get error reply")
			}
		})
	}
}

func TestStateSizeLimits_BoundariesAndBrokerLimit(t *testing.T) {
	// Helper to build valid JSON payload of exact byte length >= 2 (`""`)
	makePayload := func(size int) json.RawMessage {
		return json.RawMessage(`"` + strings.Repeat("a", size-2) + `"`)
	}

	// 1. Periodic validateStateEnvelope & decodeStateEnvelope boundaries
	envExact := validTestStateEnvelope()
	envExact.Payload = makePayload(maxStatePayloadSize)
	if err := validateStateEnvelope("test", envExact); err != nil {
		t.Fatalf("expected inner payload exactly at maxStatePayloadSize (%d) to pass: %v", maxStatePayloadSize, err)
	}
	wireExact, _ := json.Marshal(envExact)
	if len(wireExact) > maxStateWireSize {
		t.Fatalf("expected serialized envelope (%d) <= maxStateWireSize (%d)", len(wireExact), maxStateWireSize)
	}
	if _, err := decodeStateEnvelope("test", wireExact); err != nil {
		t.Fatalf("expected decodeStateEnvelope to accept valid envelope at inner payload limit: %v", err)
	}

	// Inner payload 1 byte over limit
	envOverInner := validTestStateEnvelope()
	envOverInner.Payload = makePayload(maxStatePayloadSize + 1)
	if err := validateStateEnvelope("test", envOverInner); err == nil {
		t.Fatal("expected inner payload at maxStatePayloadSize+1 to fail validation")
	}

	// Wire envelope 1 byte over maxStateWireSize
	oversizedWire := make([]byte, maxStateWireSize+1)
	if _, err := decodeStateEnvelope("test", oversizedWire); err == nil {
		t.Fatal("expected decodeStateEnvelope to reject wire message > maxStateWireSize")
	}
	if _, err := decodeStateGetReply(oversizedWire); !errors.Is(err, ErrMalformedStateGetReply) {
		t.Fatalf("expected decodeStateGetReply to reject wire message > maxStateWireSize, got %v", err)
	}

	// 2. On-demand StateGetReply with inner payload at maxStatePayloadSize fits within maxStateWireSize
	replyExact := StateGetReply{
		Version: "1.0",
		Target:  "vyos",
		State:   &envExact,
	}
	replyWire, _ := json.Marshal(replyExact)
	if len(replyWire) > maxStateWireSize {
		t.Fatalf("expected StateGetReply (%d) <= maxStateWireSize (%d)", len(replyWire), maxStateWireSize)
	}
	if _, err := decodeStateGetReply(replyWire); err != nil {
		t.Fatalf("expected decodeStateGetReply to accept reply at inner payload limit: %v", err)
	}

	// 3. Lower broker max_payload limit: PublishState rejects oversized wire message
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	client.stateMaxPayload = 1024
	smallEnv := validTestStateEnvelope()
	smallEnv.Payload = makePayload(1020) // inner < maxStatePayloadSize, but envelope > 1024 bytes
	if err := client.PublishState(context.Background(), smallEnv); err == nil {
		t.Fatal("expected PublishState to reject envelope exceeding effective broker max_payload")
	}

	// 4. Lower broker max_payload limit: on-demand handler returns small structured CodeCollectionFailed error
	client.callbacksEnabled.Store(true)
	bg := context.Background()
	client.handlerCtx.Store(&bg)

	replyCh := make(chan []byte, 1)
	client.publishStateReply = func(_ string, data []byte) {
		replyCh <- data
	}

	cb := client.bindStateGetCallback("vyos", func(_ context.Context) (StateEnvelope, error) {
		return smallEnv, nil
	})
	cb(&nats.Msg{Subject: "state.get.vyos", Reply: "_INBOX.limit", Data: nil})

	select {
	case data := <-replyCh:
		if len(data) > 1024 {
			t.Fatalf("expected small structured error reply <= 1024 bytes, got %d", len(data))
		}
		var reply StateGetReply
		if err := json.Unmarshal(data, &reply); err != nil {
			t.Fatalf("unmarshal reply: %v", err)
		}
		if reply.State != nil || reply.Error == nil || reply.Error.Code != CodeCollectionFailed {
			t.Fatalf("expected CodeCollectionFailed error reply, got %+v", reply)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for size-limit error reply")
	}
}

func TestStateSizeLimits_ConfigurableMinimumFloor(t *testing.T) {
	// 1. Value below 1 MiB minimum clamps to 1 MiB default
	cfgLow := testConfig()
	cfgLow.StateMaxWireSize = 512
	cLow, err := New(cfgLow)
	if err != nil {
		t.Fatalf("New(cfgLow): %v", err)
	}
	if cLow.stateWireLimitValue() != maxStateWireSize {
		t.Fatalf("expected wire limit clamped to %d, got %d", maxStateWireSize, cLow.stateWireLimitValue())
	}

	// 2. Value above 1 MiB increases both wire and inner payload limits
	cfgHigh := testConfig()
	cfgHigh.StateMaxWireSize = 2 * 1024 * 1024
	cHigh, err := New(cfgHigh)
	if err != nil {
		t.Fatalf("New(cfgHigh): %v", err)
	}
	if cHigh.stateWireLimitValue() != 2*1024*1024 {
		t.Fatalf("expected 2MiB wire limit, got %d", cHigh.stateWireLimitValue())
	}
	if cHigh.stateInnerPayloadLimit() != 2*1024*1024-stateWrapperOverhead {
		t.Fatalf("expected 2MiB-4KiB inner limit, got %d", cHigh.stateInnerPayloadLimit())
	}
}
