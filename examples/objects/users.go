package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/client"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrPasswordTooShort     = errors.New("password too short")
	ErrPasswordTooLong      = errors.New("password too long")
	ErrPasswordUpdateFailed = errors.New("password update failed")

	ErrDisplayNameTooShort = errors.New("display name too short")
	ErrDisplayNameTooLong  = errors.New("display name too long")

	ErrDisplayNameUpdateFailed = errors.New("display name update failed")
	ErrInvalidCredentials      = errors.New("invalid credentials")
)

const (
	UserPrefix = "demo:system:user"

	DisplayNameMinLength = 5
	DisplayNameMaxLength = 128

	PasswordMinLength = 8
	PasswordMaxLength = 128
)

type User struct {
	UUID        string    `json:"id" insi:"key:uuid,primary"`
	Email       string    `json:"email" insi:"key:email,unique"`
	Password    string    `json:"-" insi:"key:password,transform:hashPassword,noread"`
	DisplayName string    `json:"display_name" insi:"key:display_name"`
	CreatedAt   time.Time `json:"created_at" insi:"key:created_at"`
	UpdatedAt   time.Time `json:"updated_at" insi:"key:updated_at"`
}

// UserController interface defines the operations for managing users.
type UserController interface {
	NewUser(ctx context.Context, email, password, displayName string) (*User, error)
	DeleteUser(ctx context.Context, uuid string) error
	GetUserByUUID(ctx context.Context, uuid string) (*User, error)
	GetUserByEmail(ctx context.Context, email string) (*User, error)
	CheckPassword(ctx context.Context, uuid, password string) error
	GetUserByEmailAndPassword(ctx context.Context, email, password string) (*User, error)
	ListUsers(ctx context.Context) ([]*User, error)
	UpdatePassword(ctx context.Context, uuid, newPassword string) error
	UpdateEmail(ctx context.Context, uuid, newEmail string) error
	UpdateDisplayName(ctx context.Context, uuid, newDisplayName string) error
	// Note: Update operations would be added here, likely acting on the User object.
}

type ucImpl struct {
	logger        *slog.Logger
	insiClient    *client.Client
	objectManager *client.ObjectManager[User]
}

// NewUserController creates a new user controller.
func NewUserController(logger *slog.Logger, insiClient *client.Client) (UserController, error) {
	objectManager, err := client.NewObjectManagerBuilder[User](logger, insiClient, UserPrefix, client.StoreTypeValue).
		WithTransformation("hashPassword", func(ctx context.Context, input any) (any, error) {
			password, ok := input.(string)
			if !ok {
				return nil, fmt.Errorf("invalid type for password: expected string")
			}
			return internalPreparePasswordForStorage(password)
		}).
		Build()

	if err != nil {
		return nil, fmt.Errorf("failed to build object manager for users: %w", err)
	}

	return &ucImpl{
		logger:        logger,
		insiClient:    insiClient,
		objectManager: objectManager,
	}, nil
}

func (x *ucImpl) NewUser(ctx context.Context, email, password, displayName string) (*User, error) {
	if len(password) < PasswordMinLength {
		return nil, ErrPasswordTooShort
	}
	if len(password) > PasswordMaxLength {
		return nil, ErrPasswordTooLong
	}
	if len(displayName) < DisplayNameMinLength {
		return nil, ErrDisplayNameTooShort
	}
	if len(displayName) > DisplayNameMaxLength {
		return nil, ErrDisplayNameTooLong
	}
	if !strings.Contains(email, "@") {
		return nil, errors.New("invalid email format")
	}

	user := &User{
		Email:       email,
		Password:    password,
		DisplayName: displayName,
	}

	instance, err := x.objectManager.New(ctx, user)
	if err != nil {
		if errors.Is(err, client.ErrUniqueConstraintConflict) {
			return nil, errors.New("email is already taken")
		}
		x.logger.Error("failed to create user with object manager", "error", err)
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return instance.Data(), nil
}

func (x *ucImpl) DeleteUser(ctx context.Context, uuid string) error {
	return x.objectManager.Delete(ctx, uuid)
}

func (x *ucImpl) GetUserByUUID(ctx context.Context, uuid string) (*User, error) {
	instance, err := x.objectManager.GetByUUID(ctx, uuid)
	if err != nil {
		if errors.Is(err, client.ErrObjectNotFound) {
			return nil, errors.New("user not found")
		}
		return nil, err
	}
	return instance.Data(), nil
}

func (x *ucImpl) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	instance, err := x.objectManager.GetByUniqueField(ctx, "Email", email)
	if err != nil {
		if errors.Is(err, client.ErrObjectNotFound) {
			return nil, errors.New("user not found")
		}
		return nil, err
	}
	return instance.Data(), nil
}

func (x *ucImpl) CheckPassword(ctx context.Context, uuid, password string) error {
	hashedPassword, err := x.objectManager.GetField(ctx, uuid, "Password")
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			// This case should ideally not be hit if called after a user is fetched,
			// but we handle it for robustness.
			return client.ErrObjectNotFound
		}
		x.logger.Error("failed to retrieve password for user", "uuid", uuid, "error", err)
		return err
	}

	err = bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	if err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return ErrInvalidCredentials
		}
		return err
	}

	return nil
}

func (x *ucImpl) GetUserByEmailAndPassword(ctx context.Context, email, password string) (*User, error) {
	user, err := x.GetUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, client.ErrObjectNotFound) || strings.Contains(err.Error(), "user not found") {
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	if err := x.CheckPassword(ctx, user.UUID, password); err != nil {
		return nil, err
	}

	return user, nil
}

func (x *ucImpl) ListUsers(ctx context.Context) ([]*User, error) {
	instances, err := x.objectManager.List(ctx, "Email")
	if err != nil {
		return nil, fmt.Errorf("could not list users: %w", err)
	}

	var users []*User
	for _, instance := range instances {
		users = append(users, instance.Data())
	}

	return users, nil
}

func internalPreparePasswordForStorage(password string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashedPassword), nil
}

func (x *ucImpl) updateUpdatedAt(ctx context.Context, uuid string) {
	err := x.objectManager.SetField(ctx, uuid, "UpdatedAt", time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		x.logger.Error("failed to update updated_at timestamp", "uuid", uuid, "error", err)
	}
}

func (x *ucImpl) UpdatePassword(ctx context.Context, uuid, newPassword string) error {
	defer x.updateUpdatedAt(ctx, uuid)

	if len(newPassword) < PasswordMinLength {
		return ErrPasswordTooShort
	}
	if len(newPassword) > PasswordMaxLength {
		return ErrPasswordTooLong
	}

	hashedPassword, err := internalPreparePasswordForStorage(newPassword)
	if err != nil {
		return fmt.Errorf("failed to hash new password: %w", err)
	}

	// We don't use CAS here because the user is already authenticated.
	// We are setting the password directly.
	return x.objectManager.SetField(ctx, uuid, "Password", hashedPassword)
}

func (x *ucImpl) UpdateEmail(ctx context.Context, uuid, newEmail string) error {
	defer x.updateUpdatedAt(ctx, uuid)

	if !strings.Contains(newEmail, "@") {
		return errors.New("invalid email format")
	}

	// Check if the new email is already taken
	err := x.objectManager.SetUniqueNX(ctx, uuid, "Email", newEmail)
	if err != nil {
		if errors.Is(err, client.ErrConflict) {
			return errors.New("email is already taken")
		}
		return fmt.Errorf("failed to set uniqueness for new email: %w", err)
	}

	// Get the old email to remove its unique key
	currentUser, err := x.GetUserByUUID(ctx, uuid)
	if err != nil {
		// If we fail here, we should try to roll back the SetNX
		_ = x.objectManager.DeleteUnique(ctx, "Email", newEmail)
		return fmt.Errorf("could not retrieve user to update email: %w", err)
	}

	// Set the new email on the user object
	if err := x.objectManager.SetField(ctx, uuid, "Email", newEmail); err != nil {
		// Rollback SetNX
		_ = x.objectManager.DeleteUnique(ctx, "Email", newEmail)
		return fmt.Errorf("failed to set new email field: %w", err)
	}

	// Delete the old unique email key
	if err := x.objectManager.DeleteUnique(ctx, "Email", currentUser.Email); err != nil {
		// This is not ideal, we have a dangling old unique key.
		x.logger.Error("failed to delete old unique email key", "email", currentUser.Email, "error", err)
	}

	return nil
}

func (x *ucImpl) UpdateDisplayName(ctx context.Context, uuid, newDisplayName string) error {
	defer x.updateUpdatedAt(ctx, uuid)

	if len(newDisplayName) < DisplayNameMinLength {
		return ErrDisplayNameTooShort
	}
	if len(newDisplayName) > DisplayNameMaxLength {
		return ErrDisplayNameTooLong
	}

	return x.objectManager.SetField(ctx, uuid, "DisplayName", newDisplayName)
}
