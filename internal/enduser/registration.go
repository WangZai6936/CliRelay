package enduser

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

var (
	ErrUsernameTaken           = errors.New("username already taken")
	ErrRegistrationUnavailable = errors.New("registration unavailable")
	registrationUsername       = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{2,47}$`)
)

// RegistrationOptions contains trusted server-side signup policy.
type RegistrationOptions struct {
	TenantID            string
	PermissionProfileID string
}

// Register creates an active portal account and its default key atomically.
// A chosen username must be available globally, matching the portal login lookup;
// unlike admin provisioning, signup never silently appends a numeric suffix.
func (s *Service) Register(ctx context.Context, options RegistrationOptions, username, displayName, password string) (User, error) {
	if s == nil || s.db == nil {
		return User{}, ErrRegistrationUnavailable
	}
	username = NormalizeUsername(username)
	if !registrationUsername.MatchString(username) {
		return User{}, fmt.Errorf("%w: username must be 3-48 ASCII letters, digits, dots, underscores or hyphens and start with a letter or digit", ErrValidation)
	}
	displayName = strings.TrimSpace(displayName)
	if displayName == "" {
		displayName = username
	}
	if len(displayName) > 128 || !utf8.ValidString(displayName) {
		return User{}, fmt.Errorf("%w: display_name must not exceed 128 bytes", ErrValidation)
	}
	tenantID := strings.TrimSpace(options.TenantID)
	if requireUUID(tenantID) != nil {
		return User{}, ErrRegistrationUnavailable
	}
	if err := s.ensureTenantActive(ctx, tenantID); err != nil {
		return User{}, fmt.Errorf("%w: %v", ErrRegistrationUnavailable, err)
	}
	hash, err := HashPassword(password)
	if err != nil {
		return User{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, err
	}
	defer func() { _ = tx.Rollback() }()

	profileID := strings.TrimSpace(options.PermissionProfileID)
	if profileID != "" {
		var found int
		if err = tx.QueryRowContext(ctx, `SELECT 1 FROM api_key_permission_profiles WHERE tenant_id = ? AND id = ?`, tenantID, profileID).Scan(&found); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return User{}, ErrRegistrationUnavailable
			}
			return User{}, err
		}
	}

	userID := uuid.NewString()
	inserted, err := tx.ExecContext(ctx, `
		INSERT INTO end_users (id, tenant_id, username, username_normalized, display_name, password_hash, must_change_password, permission_profile_id)
		VALUES (?, ?, ?, ?, ?, ?, false, ?)
		ON CONFLICT (username_normalized) DO NOTHING
	`, userID, tenantID, username, username, displayName, hash, profileID)
	if err != nil {
		return User{}, err
	}
	count, err := inserted.RowsAffected()
	if err != nil {
		return User{}, err
	}
	if count == 0 {
		return User{}, ErrUsernameTaken
	}
	if _, _, err = s.insertDefaultKey(ctx, tx, tenantID, userID, displayName); err != nil {
		return User{}, err
	}
	// Read before committing so even a failed account read rolls back the key
	// and account together. No credential material is returned by registration.
	user, err := scanUser(tx.QueryRowContext(ctx, userSelect+` WHERE tenant_id = ? AND id = ?`, tenantID, userID))
	if err != nil {
		return User{}, err
	}
	if err = commitKeyChange(ctx, tx, tenantID); err != nil {
		return User{}, err
	}
	return user, nil
}
