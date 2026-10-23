package enduser

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	"golang.org/x/crypto/bcrypt"
)

func registrationTestService(t *testing.T) (*Service, *sql.DB) {
	t.Helper()
	db := openEndUserTestDB(t)
	if _, err := db.Exec(`CREATE TABLE tenants (id TEXT PRIMARY KEY, status TEXT, type TEXT, expires_at DATETIME);
		INSERT INTO tenants (id, status, type) VALUES ('00000000-0000-0000-0000-000000000001', 'active', 'system')`); err != nil {
		t.Fatal(err)
	}
	return NewService(db), db
}

func TestRegisterCreatesAccountAndDefaultKeyWithServerPolicy(t *testing.T) {
	svc, db := registrationTestService(t)
	tenantID := uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants (id, status, type) VALUES (?, 'active', 'business');
		INSERT INTO api_key_permission_profiles (tenant_id, id, name, daily_limit) VALUES (?, 'signup', 'Signup', 25)`, tenantID, tenantID); err != nil {
		t.Fatal(err)
	}
	user, err := svc.Register(context.Background(), RegistrationOptions{TenantID: tenantID, PermissionProfileID: "signup"}, "  Alice.ONE  ", "Alice", "Signup-Password-123!")
	if err != nil {
		t.Fatal(err)
	}
	if user.Username != "alice.one" || user.DisplayName != "Alice" || user.TenantID != tenantID || user.PermissionProfileID != "signup" || user.Status != "active" || user.MustChangePassword {
		t.Fatalf("unexpected public account: %+v", user)
	}
	var hash string
	if err := db.QueryRow(`SELECT password_hash FROM end_users WHERE id = ?`, user.ID).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte("Signup-Password-123!")) != nil {
		t.Fatal("password is not stored as a valid bcrypt hash")
	}
	keys, err := svc.ListKeys(context.Background(), tenantID, user.ID)
	if err != nil || len(keys) != 1 {
		t.Fatalf("default key: %v, count %d", err, len(keys))
	}
	if keys[0].EndUserID != user.ID || keys[0].TenantID != tenantID || !keys[0].IsDefault {
		t.Fatalf("key ownership: %+v", keys[0])
	}
	_, err = svc.Register(context.Background(), RegistrationOptions{TenantID: identity.SystemTenantID}, "ALICE.ONE", "Someone else", "Signup-Password-123!")
	if !errors.Is(err, ErrUsernameTaken) {
		t.Fatalf("global duplicate must fail without renaming: %v", err)
	}
	var users, keyCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM end_users`).Scan(&users)
	_ = db.QueryRow(`SELECT COUNT(*) FROM api_keys`).Scan(&keyCount)
	if users != 1 || keyCount != 1 {
		t.Fatalf("duplicate created side effects: %d users / %d keys", users, keyCount)
	}
}

func TestRegisterValidationLeavesNoAccountOrKey(t *testing.T) {
	tests := []struct{ name, username, displayName, password string }{
		{"empty username", "", "", "Signup-Password-123!"},
		{"short username", "ab", "", "Signup-Password-123!"},
		{"invalid username", "alice@example.com", "", "Signup-Password-123!"},
		{"long username", strings.Repeat("a", 49), "", "Signup-Password-123!"},
		{"invalid prefix", "_alice", "", "Signup-Password-123!"},
		{"long display name", "alice", strings.Repeat("名", 43), "Signup-Password-123!"},
		{"empty password", "alice", "", ""},
		{"weak password", "alice", "", "password123456"},
		{"long password", "alice", "", strings.Repeat("A", 72) + "a!"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			svc, db := registrationTestService(t)
			_, err := svc.Register(context.Background(), RegistrationOptions{TenantID: identity.SystemTenantID}, test.username, test.displayName, test.password)
			if !errors.Is(err, ErrValidation) {
				t.Fatalf("validation error: %v", err)
			}
			var count int
			if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM end_users) + (SELECT COUNT(*) FROM api_keys)`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("invalid signup wrote rows: %d, %v", count, err)
			}
		})
	}
}

func TestRegisterRejectsUnavailableTenantAndCrossTenantProfile(t *testing.T) {
	svc, db := registrationTestService(t)
	ctx := context.Background()
	otherTenant := uuid.NewString()
	if _, err := db.Exec(`INSERT INTO tenants (id, status, type, expires_at) VALUES (?, 'active', 'business', ?);
		INSERT INTO api_key_permission_profiles (tenant_id, id) VALUES (?, 'other-profile')`, otherTenant, time.Now().Add(-time.Hour), otherTenant); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []RegistrationOptions{
		{TenantID: "invalid"},
		{TenantID: uuid.NewString()},
		{TenantID: otherTenant},
		{TenantID: identity.SystemTenantID, PermissionProfileID: "missing-profile"},
		{TenantID: identity.SystemTenantID, PermissionProfileID: "other-profile"},
	} {
		if _, err := svc.Register(ctx, policy, "alice", "", "Signup-Password-123!"); !errors.Is(err, ErrRegistrationUnavailable) {
			t.Fatalf("policy %+v accepted or leaked internal error: %v", policy, err)
		}
	}
	if _, err := db.Exec(`UPDATE tenants SET status = 'suspended' WHERE id = ?`, identity.SystemTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Register(ctx, RegistrationOptions{TenantID: identity.SystemTenantID}, "alice", "", "Signup-Password-123!"); !errors.Is(err, ErrRegistrationUnavailable) {
		t.Fatalf("suspended tenant: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM end_users`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("unavailable signup wrote account: %d, %v", count, err)
	}
}

func TestRegisterRollsBackAccountWhenDefaultKeyFails(t *testing.T) {
	svc, db := registrationTestService(t)
	if _, err := db.Exec(`CREATE TRIGGER reject_signup_key BEFORE INSERT ON api_keys BEGIN SELECT RAISE(ABORT, 'test key failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Register(context.Background(), RegistrationOptions{TenantID: identity.SystemTenantID}, "alice", "", "Signup-Password-123!"); err == nil {
		t.Fatal("expected key creation failure")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM end_users`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("orphan account after rollback: %d, %v", count, err)
	}
}

// The production-engine check includes concurrent duplicate signup, then the
// existing login and authenticated key listing used by the user portal.
func TestPostgresPublicRegistrationLifecycle(t *testing.T) {
	db := openLegacyLockPostgresDB(t)
	ctx := context.Background()
	if err := identity.NewService(db).Bootstrap(ctx, "Registration-Admin-123!"); err != nil {
		t.Fatal(err)
	}
	svc := NewService(db)
	var wg sync.WaitGroup
	errorsCh := make(chan error, 4)
	for range 4 {
		wg.Go(func() {
			_, err := svc.Register(ctx, RegistrationOptions{TenantID: identity.SystemTenantID}, "public_alice", "", "Signup-Password-123!")
			errorsCh <- err
		})
	}
	wg.Wait()
	close(errorsCh)
	successes := 0
	for err := range errorsCh {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrUsernameTaken) {
			t.Fatalf("concurrent signup: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful signups = %d, want 1", successes)
	}
	login, err := svc.Login(ctx, "PUBLIC_ALICE", "Signup-Password-123!", "registration-test")
	if err != nil || login.MustChangePassword || login.User.DisplayName != "public_alice" {
		t.Fatalf("signup login: %+v, %v", login, err)
	}
	user, _, err := svc.Authenticate(ctx, login.AccessToken)
	if err != nil || user.ID != login.User.ID {
		t.Fatalf("authenticate new account: %v", err)
	}
	keys, err := svc.ListKeys(ctx, user.TenantID, user.ID)
	if err != nil || len(keys) != 1 {
		t.Fatalf("portal key list: %v / %d", err, len(keys))
	}
}
