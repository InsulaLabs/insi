package etok

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/InsulaLabs/insula/security/badge"
	"github.com/jellydator/ttlcache/v3"
)

// mockBadge is a mock implementation of badge.Badge for testing.
type mockBadge struct {
	signFunc         func(payload *string) (badge.SignedPackage, error)
	verifyFunc       func(p badge.SignedPackage) (bool, error)
	getVersionFunc   func() badge.BadgeVersion
	getIDFunc        func() string
	encryptBadgeFunc func(secret []byte) ([]byte, error)
	encryptDataFunc  func(data []byte) ([]byte, error)
	decryptDataFunc  func(data []byte) ([]byte, error)
}

func (m *mockBadge) Sign(payload *string) (badge.SignedPackage, error) {
	if m.signFunc != nil {
		return m.signFunc(payload)
	}
	// Default behavior: simple sign
	if payload == nil {
		return badge.SignedPackage{}, fmt.Errorf("payload is nil")
	}
	return badge.SignedPackage{
		PublicKey: []byte("mock-public-key"),
		Signature: []byte("mock-signature"),
		Message:   []byte(*payload),
	}, nil
}

func (m *mockBadge) Verify(p badge.SignedPackage) (bool, error) {
	if m.verifyFunc != nil {
		return m.verifyFunc(p)
	}
	// Default behavior: always verify true
	return true, nil
}

func (m *mockBadge) GetID() string {
	if m.getIDFunc != nil {
		return m.getIDFunc()
	}
	return "mock-badge-id"
}

func (m *mockBadge) Type() string {
	return "mock"
}

func (m *mockBadge) GetVersion() badge.BadgeVersion {
	if m.getVersionFunc != nil {
		return m.getVersionFunc()
	}
	return badge.BadgeVersion1 // Default version
}

func (m *mockBadge) EncryptBadge(secret []byte) ([]byte, error) {
	if m.encryptBadgeFunc != nil {
		return m.encryptBadgeFunc(secret)
	}
	return []byte("mock-encrypted-badge"), nil
}

func (m *mockBadge) EncryptData(data []byte) ([]byte, error) {
	if m.encryptDataFunc != nil {
		return m.encryptDataFunc(data)
	}
	return []byte("mock-encrypted-data"), nil
}

func (m *mockBadge) DecryptData(data []byte) ([]byte, error) {
	if m.decryptDataFunc != nil {
		return m.decryptDataFunc(data)
	}
	return []byte("mock-decrypted-data"), nil
}

func newTestEtok(t *testing.T, mockBadgeOverride badge.Badge) *etok {
	t.Helper()
	var testBadge badge.Badge
	if mockBadgeOverride != nil {
		testBadge = mockBadgeOverride
	} else {
		testBadge = &mockBadge{} // Default mock
	}

	testLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// Disable touch on hit for predictable TTL testing
	tokCache := ttlcache.New[string, tokCacheItem](
		ttlcache.WithDisableTouchOnHit[string, tokCacheItem](),
		ttlcache.WithTTL[string, tokCacheItem](time.Hour*24), // Default TTL for items not specifically set otherwise
	)
	go tokCache.Start()

	return &etok{
		identity:        testBadge,
		logger:          testLogger.WithGroup("etok_test"),
		instanceUUID:    "test-instance-uuid",
		prefixSignedAt:  "test-instance-uuid:signed_at",
		prefixSignature: "test-instance-uuid:signature",
		tokCache:        tokCache,
	}
}

func TestNewScopeToken(t *testing.T) {
	defaultSignFunc := func(payload *string) (badge.SignedPackage, error) {
		if payload == nil {
			return badge.SignedPackage{}, fmt.Errorf("payload is nil for sign")
		}
		return badge.SignedPackage{PublicKey: []byte("pk"), Signature: []byte("sig"), Message: []byte(*payload)}, nil
	}

	tests := []struct {
		name            string
		entityRequester string
		apiUuidOfEntity string
		scopes          map[string]string
		ttl             time.Duration
		mockBadge       badge.Badge
		wantErr         bool
		errContains     string
	}{
		{
			name:            "success",
			entityRequester: "user1",
			apiUuidOfEntity: "uuid1",
			scopes:          map[string]string{"read": "data"},
			ttl:             time.Minute,
			mockBadge:       &mockBadge{signFunc: defaultSignFunc},
			wantErr:         false,
		},
		{
			name:            "error ttl is 0",
			entityRequester: "user1",
			apiUuidOfEntity: "uuid1",
			scopes:          map[string]string{"read": "data"},
			ttl:             0,
			mockBadge:       &mockBadge{signFunc: defaultSignFunc},
			wantErr:         true,
			errContains:     "ttl must be greater than 0",
		},
		{
			name:            "error entityRequester empty",
			entityRequester: "",
			apiUuidOfEntity: "uuid1",
			scopes:          map[string]string{"read": "data"},
			ttl:             time.Minute,
			mockBadge:       &mockBadge{signFunc: defaultSignFunc},
			wantErr:         true,
			errContains:     "entityRequester must be set",
		},
		{
			name:            "error apiUuidOfEntity empty",
			entityRequester: "user1",
			apiUuidOfEntity: "",
			scopes:          map[string]string{"read": "data"},
			ttl:             time.Minute,
			mockBadge:       &mockBadge{signFunc: defaultSignFunc},
			wantErr:         true,
			errContains:     "apiUuidOfEntity must be set",
		},
		{
			name:            "error scopes empty",
			entityRequester: "user1",
			apiUuidOfEntity: "uuid1",
			scopes:          map[string]string{},
			ttl:             time.Minute,
			mockBadge:       &mockBadge{signFunc: defaultSignFunc},
			wantErr:         true,
			errContains:     "scopes must be set",
		},
		{
			name:            "error signing failed",
			entityRequester: "user1",
			apiUuidOfEntity: "uuid1",
			scopes:          map[string]string{"read": "data"},
			ttl:             time.Minute,
			mockBadge: &mockBadge{signFunc: func(payload *string) (badge.SignedPackage, error) {
				return badge.SignedPackage{}, fmt.Errorf("signing failed")
			}},
			wantErr:     true,
			errContains: "signing failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEtok(t, tt.mockBadge)
			defer e.tokCache.Stop() // Ensure cache is stopped after test

			token, err := e.NewScopeToken(tt.entityRequester, tt.apiUuidOfEntity, tt.scopes, tt.ttl)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewScopeToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewScopeToken() error = %v, want errContains %q", err, tt.errContains)
				}
				if token != "" {
					t.Errorf("NewScopeToken() token = %q, want empty string on error", token)
				}
			} else {
				if token == "" {
					t.Error("NewScopeToken() token is empty, want non-empty")
				}
				// Verify item in cache
				item := e.tokCache.Get(token)
				if item == nil {
					t.Fatalf("NewScopeToken() token %q not found in cache", token)
				}
				if !reflect.DeepEqual(item.Value().requestedScopes, tt.scopes) {
					t.Errorf("NewScopeToken() cached scopes = %v, want %v", item.Value().requestedScopes, tt.scopes)
				}
				if item.Value().entityRequester != tt.entityRequester {
					t.Errorf("NewScopeToken() cached entityRequester = %s, want %s", item.Value().entityRequester, tt.entityRequester)
				}
				if item.Value().apiUuidOfEntity != tt.apiUuidOfEntity {
					t.Errorf("NewScopeToken() cached apiUuidOfEntity = %s, want %s", item.Value().apiUuidOfEntity, tt.apiUuidOfEntity)
				}
			}
		})
	}
}

func TestVerifyScopeToken(t *testing.T) {
	entityReq := "user1"
	apiUUID := "uuid1"
	scopes := map[string]string{"read": "data"}
	validTTL := time.Minute

	mockSig := []byte("mock-signature")
	mockPubKey := []byte("mock-public-key")

	successVerifyFunc := func(p badge.SignedPackage) (bool, error) {
		if !bytes.Equal(p.Signature, mockSig) {
			return false, fmt.Errorf("Verify: unexpected signature: got %s, want %s", string(p.Signature), string(mockSig))
		}
		if !bytes.Equal(p.PublicKey, mockPubKey) {
			return false, fmt.Errorf("Verify: unexpected public key: got %s, want %s", string(p.PublicKey), string(mockPubKey))
		}
		// Note: In a real scenario, the message in SignedPackage would be the original scope string.
		// For this mock, we are not deeply checking p.Message against the expected scopes string here,
		// as the Sign mock already ensures this.
		return true, nil
	}

	signFuncReturningMock := func(payload *string) (badge.SignedPackage, error) {
		return badge.SignedPackage{PublicKey: mockPubKey, Signature: mockSig, Message: []byte(*payload)}, nil
	}

	tests := []struct {
		name              string
		setupEtok         func(t *testing.T) (*etok, string) // Returns etok instance and token for test
		mockVerify        func(p badge.SignedPackage) (bool, error)
		tokenToVerify     string // if set, overrides token from setupEtok
		wantEntity        string
		wantApiUUID       string
		wantScopes        map[string]string
		wantErr           bool
		errContains       string
		verifyTokenIsGone bool // Check if token is deleted after successful verification
	}{
		{
			name: "success",
			setupEtok: func(t *testing.T) (*etok, string) {
				e := newTestEtok(t, &mockBadge{signFunc: signFuncReturningMock, verifyFunc: successVerifyFunc})
				token, err := e.NewScopeToken(entityReq, apiUUID, scopes, validTTL)
				if err != nil {
					t.Fatalf("Failed to create token for success test: %v", err)
				}
				return e, token
			},
			wantEntity:        entityReq,
			wantApiUUID:       apiUUID,
			wantScopes:        scopes,
			wantErr:           false,
			verifyTokenIsGone: true,
		},
		{
			name: "token not found",
			setupEtok: func(t *testing.T) (*etok, string) {
				// No specific badge behavior needed as token won't be found
				e := newTestEtok(t, &mockBadge{})
				return e, "nonexistent-token"
			},
			tokenToVerify: "nonexistent-token",
			wantErr:       true,
			errContains:   "token not found",
		},
		{
			name: "verification fails - signature invalid",
			setupEtok: func(t *testing.T) (*etok, string) {
				mockBadgeInstance := &mockBadge{
					signFunc: signFuncReturningMock,
					verifyFunc: func(p badge.SignedPackage) (bool, error) {
						return false, nil // Verification returns false, no error
					},
				}
				e := newTestEtok(t, mockBadgeInstance)
				token, _ := e.NewScopeToken(entityReq, apiUUID, scopes, validTTL)
				return e, token
			},
			wantErr:     true,
			errContains: "signature verification failed",
		},
		{
			name: "verification error",
			setupEtok: func(t *testing.T) (*etok, string) {
				mockBadgeInstance := &mockBadge{
					signFunc: signFuncReturningMock,
					verifyFunc: func(p badge.SignedPackage) (bool, error) {
						return false, fmt.Errorf("internal verify error")
					},
				}
				e := newTestEtok(t, mockBadgeInstance)
				token, _ := e.NewScopeToken(entityReq, apiUUID, scopes, validTTL)
				return e, token
			},
			wantErr:     true,
			errContains: "signature verification failed: internal verify error",
		},
		{
			name: "token expired",
			setupEtok: func(t *testing.T) (*etok, string) {
				mockBadgeInstance := &mockBadge{signFunc: signFuncReturningMock, verifyFunc: successVerifyFunc}
				e := newTestEtok(t, mockBadgeInstance)
				tokenShortTTL := time.Millisecond * 50
				token, _ := e.NewScopeToken(entityReq, apiUUID, scopes, tokenShortTTL)
				time.Sleep(tokenShortTTL + time.Millisecond*100) // Ensure token is expired
				return e, token
			},
			wantErr:     true,
			errContains: "token not found", // Expired tokens are removed by ttlcache
		},
		{
			name: "token already used (deleted)",
			setupEtok: func(t *testing.T) (*etok, string) {
				e := newTestEtok(t, &mockBadge{signFunc: signFuncReturningMock, verifyFunc: successVerifyFunc})
				token, _ := e.NewScopeToken(entityReq, apiUUID, scopes, validTTL)
				// First verification (should succeed and delete token)
				_, _, _, err := e.VerifyScopeToken(token)
				if err != nil {
					t.Fatalf("Preliminary verification failed: %v", err)
				}
				return e, token // Return the same token to be verified again
			},
			wantErr:     true,
			errContains: "token not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, tokenToTest := tt.setupEtok(t)
			defer e.tokCache.Stop()

			if tt.tokenToVerify != "" { // Override token if specified in test case
				tokenToTest = tt.tokenToVerify
			}

			entity, recAPIUUID, recScopes, err := e.VerifyScopeToken(tokenToTest)

			if (err != nil) != tt.wantErr {
				t.Errorf("VerifyScopeToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("VerifyScopeToken() error = %v, want errContains %q", err, tt.errContains)
				}
			} else {
				if entity != tt.wantEntity {
					t.Errorf("VerifyScopeToken() entity = %q, want %q", entity, tt.wantEntity)
				}
				if recAPIUUID != tt.wantApiUUID {
					t.Errorf("VerifyScopeToken() apiUUID = %q, want %q", recAPIUUID, tt.wantApiUUID)
				}
				if !reflect.DeepEqual(recScopes, tt.wantScopes) {
					t.Errorf("VerifyScopeToken() scopes = %v, want %v", recScopes, tt.wantScopes)
				}
				if tt.verifyTokenIsGone {
					if item := e.tokCache.Get(tokenToTest); item != nil {
						t.Errorf("VerifyScopeToken() token %q should have been deleted from cache but was found", tokenToTest)
					}
				}
			}
		})
	}
}
