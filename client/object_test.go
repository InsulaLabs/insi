package client_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TestObject defines the structure used for testing the ObjectManager.
type TestObject struct {
	ID     string `insi:"key:id,primary"`
	Email  string `insi:"key:email,unique"`
	Name   string `insi:"key:name"`
	Secret string `insi:"key:secret,transform:reverse,noread"`
}

// ObjectManagerTestSuite is a test suite for the ObjectManager.
type ObjectManagerTestSuite struct {
	suite.Suite
	ctx          context.Context
	insiClient   *client.Client
	logger       *slog.Logger
	manager      *client.ObjectManager[TestObject]
	cacheManager *client.ObjectManager[TestObject]
}

// SetupSuite initializes the test suite, setting up the logger, insi client,
// and ObjectManager instances for both value and cache stores.
func (s *ObjectManagerTestSuite) SetupSuite() {
	s.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(s.logger)

	apiKey := os.Getenv("INSI_API_KEY")
	if apiKey == "" {
		s.T().Fatal("INSI_API_KEY environment variable not set. Please set it to run these tests.")
	}

	insiCfg := &client.Config{
		Logger:                 s.logger.WithGroup("insi-client"),
		ConnectionType:         client.ConnectionTypeRandom,
		SkipVerify:             false,
		EnableLeaderStickiness: true,
		ApiKey:                 apiKey,
		Endpoints: []client.Endpoint{
			{PublicBinding: "db-0.insula.dev:443", PrivateBinding: "db-0.insula.dev:444", ClientDomain: "db-0.insula.dev", Logger: s.logger},
			{PublicBinding: "db-1.insula.dev:443", PrivateBinding: "db-1.insula.dev:444", ClientDomain: "db-1.insula.dev", Logger: s.logger},
			{PublicBinding: "db-2.insula.dev:443", PrivateBinding: "db-2.insula.dev:444", ClientDomain: "db-2.insula.dev", Logger: s.logger},
		},
	}

	var err error
	s.insiClient, err = client.NewClient(insiCfg)
	require.NoError(s.T(), err)

	s.ctx = context.Background()

	reverseTransform := func(ctx context.Context, input any) (any, error) {
		s, ok := input.(string)
		if !ok {
			return nil, fmt.Errorf("invalid type for secret, expected string")
		}
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
	}

	// Value store manager
	builder, err := client.NewObjectManagerBuilder[TestObject](s.logger, s.insiClient, "test_object_value", client.StoreTypeValue).
		WithTransformation("reverse", reverseTransform).
		Build()
	require.NoError(s.T(), err)
	s.manager = builder

	// Cache store manager
	cacheBuilder, err := client.NewObjectManagerBuilder[TestObject](s.logger, s.insiClient, "test_object_cache", client.StoreTypeCache).
		WithTransformation("reverse", reverseTransform).
		Build()
	require.NoError(s.T(), err)
	s.cacheManager = cacheBuilder
}

// TestObjectManagerSuite runs the test suite.
func TestObjectManagerSuite(t *testing.T) {
	suite.Run(t, new(ObjectManagerTestSuite))
}

// TestObjectManager_CRUD performs a full suite of CRUD tests on both the value and cache stores.
func (s *ObjectManagerTestSuite) TestObjectManager_CRUD() {
	managers := map[string]*client.ObjectManager[TestObject]{
		"value_store": s.manager,
		"cache_store": s.cacheManager,
	}

	for name, manager := range managers {
		s.T().Run(name, func(t *testing.T) {
			require := require.New(t)

			// --- Create ---
			email := fmt.Sprintf("test-%d@example.com", time.Now().UnixNano())
			secret := "this-is-a-secret"
			obj := &TestObject{
				Email:  email,
				Name:   "Test User",
				Secret: secret,
			}
			instance, err := manager.New(s.ctx, obj)
			require.NoError(err)
			require.NotNil(instance)
			require.NotEmpty(instance.Data().ID)
			t.Logf("Created object with ID: %s", instance.Data().ID)

			// Defer cleanup to ensure deletion even if tests fail mid-way.
			defer func() {
				err := manager.Delete(s.ctx, instance.Data().ID)
				// We don't want to fail the test if cleanup fails, just log it.
				if err != nil && err != client.ErrObjectNotFound {
					t.Logf("Cleanup failed for object %s: %v", instance.Data().ID, err)
				}
			}()

			// --- Get by UUID ---
			retrieved, err := manager.GetByUUID(s.ctx, instance.Data().ID)
			require.NoError(err)
			require.NotNil(retrieved)
			require.Equal(instance.Data().ID, retrieved.Data().ID)
			require.Equal(obj.Email, retrieved.Data().Email)
			require.Equal(obj.Name, retrieved.Data().Name)
			require.Empty(retrieved.Data().Secret, "Secret field should be empty due to noread tag")

			// --- Get by Unique Field ---
			retrievedByEmail, err := manager.GetByUniqueField(s.ctx, "Email", email)
			require.NoError(err)
			require.NotNil(retrievedByEmail)
			require.Equal(instance.Data().ID, retrievedByEmail.Data().ID)
			require.Empty(retrievedByEmail.Data().Secret, "Secret field should be empty due to noread tag")

			// --- Get noread field directly ---
			retrievedSecret, err := manager.GetField(s.ctx, instance.Data().ID, "Secret")
			require.NoError(err)

			// Reverse the original secret to check against the transformed value
			runes := []rune(secret)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			expectedSecret := string(runes)
			require.Equal(expectedSecret, retrievedSecret, "GetField should return the transformed secret")

			// --- Unique constraint ---
			anotherObj := &TestObject{
				Email: email, // Same email
				Name:  "Another User",
			}
			_, err = manager.New(s.ctx, anotherObj)
			require.Error(err)
			require.ErrorIs(err, client.ErrUniqueConstraintConflict, "Expected a conflict error when creating an object with a duplicate unique field")

			// --- Update field ---
			newName := "Updated Test User"
			err = manager.SetField(s.ctx, instance.Data().ID, "Name", newName)
			require.NoError(err)

			updated, err := manager.GetByUUID(s.ctx, instance.Data().ID)
			require.NoError(err)
			require.Equal(newName, updated.Data().Name)

			// --- Delete ---
			err = manager.Delete(s.ctx, instance.Data().ID)
			require.NoError(err)

			// --- Verify Deletion ---
			_, err = manager.GetByUUID(s.ctx, instance.Data().ID)
			require.Error(err)
			require.ErrorIs(err, client.ErrObjectNotFound, "Expected object not found after deletion")

			// --- Verify Unique Key Deletion ---
			_, err = manager.GetByUniqueField(s.ctx, "Email", email)
			require.Error(err)
			require.ErrorIs(err, client.ErrObjectNotFound, "Expected unique key to be deleted along with the object")
		})
	}
}

// TestList verifies the List functionality for both store types.
func (s *ObjectManagerTestSuite) TestList() {
	managers := map[string]*client.ObjectManager[TestObject]{
		"value_store": s.manager,
		"cache_store": s.cacheManager,
	}

	for name, manager := range managers {
		s.T().Run(name, func(t *testing.T) {
			require := require.New(t)

			// --- Cleanup before test ---
			all, err := manager.List(s.ctx, "Email")
			require.NoError(err)
			for _, item := range all {
				err := manager.Delete(s.ctx, item.Data().ID)
				require.NoError(err)
			}

			// --- Create a few objects ---
			var createdIDs []string
			for i := 0; i < 3; i++ {
				email := fmt.Sprintf("list-test-%s-%d@example.com", name, time.Now().UnixNano())
				obj := &TestObject{
					Email: email,
					Name:  fmt.Sprintf("List Test User %d", i),
				}
				instance, err := manager.New(s.ctx, obj)
				require.NoError(err)
				createdIDs = append(createdIDs, instance.Data().ID)
			}

			// Defer cleanup.
			defer func() {
				for _, id := range createdIDs {
					_ = manager.Delete(s.ctx, id)
				}
			}()

			// --- List ---
			instances, err := manager.List(s.ctx, "Email")
			require.NoError(err)
			require.Len(instances, 3, "Expected to list all created instances")

			foundIDs := make(map[string]bool)
			for _, inst := range instances {
				foundIDs[inst.Data().ID] = true
			}

			for _, id := range createdIDs {
				require.True(foundIDs[id], "Did not find created ID in list")
			}
		})
	}
}
