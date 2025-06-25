# Insula Client Object System

The Insula Client Object System is a powerful, reflection-based Object-Relational Mapping (ORM)-like layer that simplifies interaction with the Insula key-value store. It allows developers to work with native Go structs, while the system automatically handles the underlying key generation, data serialization, and indexing.

## Core Concepts

- **`ObjectManager[T]`**: The central component. It is a generic controller for a specific Go struct type `T`. All operations for a given object type (create, read, update, delete) are performed through its manager.
- **`ObjectManagerBuilder[T]`**: A fluent builder used to construct and configure an `ObjectManager`. It allows you to specify the storage backend, register data transformers, and define the object's key prefix.
- **`ObjectInstance[T]`**: A wrapper around a retrieved struct instance. It holds a pointer to the manager that fetched it and the actual data (`*T`), accessible via the `.Data()` method.

---

## 1. Defining a Managed Object

To make a Go struct manageable by the system, you must annotate its fields with `insi` struct tags.

### Example Object Definition

```go
import "time"

type User struct {
    // This is the primary key. It must be a string.
    // The system will automatically populate it with a UUID on creation.
    ID          string `insi:"key:id,primary"`

    // A unique field. The system will create and maintain a secondary index
    // to allow lookups by this field.
    Username    string `insi:"key:username,unique"`

    // A field that will be transformed before being stored.
    PasswordHash string `insi:"key:password,transform:hash,noread"`

    // A standard, readable/writable field.
    Email       string `insi:"key:email"`

    // These fields are automatically managed by the system.
    // They are not required in the struct definition but can be included if needed.
    // CreatedAt   time.Time `insi:"key:created_at"`
    // UpdatedAt   time.Time `insi:"key:updated_at"`
}
```

### Struct Tag Reference

- `key:<name>`: Specifies the name to be used for this field in the underlying key-value store. If omitted, the field name is converted to lowercase (e.g., `Username` becomes `username`).
- `primary`: Designates this field as the primary key for the object.
    - There must be exactly one primary key per struct.
    - The field must be of type `string`. The system will populate it with a new UUID upon object creation.
- `unique`: Creates a secondary index for this field, allowing fast lookups using `GetByUniqueField`. The system guarantees the uniqueness of this field across all objects of this type.
- `transform:<name>`: Applies a named `TransformationFunc` to the field's value before it is stored. The name must correspond to a function registered with the `ObjectManagerBuilder`.
- `noread`: This field will not be read back from the data store when an object is retrieved. This is useful for write-only fields like passwords, where you only store a transformed hash but never read it back directly.
- `"-"`: If the tag is `insi:"-"`, the object manager will ignore this field completely.

---

## 2. Creating an Object Manager

An `ObjectManager` is created using its builder.

```go
import (
    "log/slog"
    "github.com/your-repo/insi/client"
)

// Example password hashing function
func hashPassword(ctx context.Context, input any) (any, error) {
    password, ok := input.(string)
    if !ok {
        return nil, errors.New("input must be a string")
    }
    // In a real application, use a strong hashing algorithm like bcrypt
    return "hashed_" + password, nil
}


// ... inside your application setup ...
logger := slog.Default()
insiClient, err := client.New(...) // Your Insula client instance

// Create a builder for the User object
// This will store User objects in the main persistent "value" store.
userManagerBuilder := client.NewObjectManagerBuilder[User](
    logger,
    insiClient,
    "users", // This is the prefix for all keys related to User objects
    client.StoreTypeValue,
)

// Register the transformation function named "hash"
userManagerBuilder.WithTransformation("hash", hashPassword)

// Build the manager
userManager, err := userManagerBuilder.Build()
if err != nil {
    // Handle error
}
```

### Builder Configuration

- `NewObjectManagerBuilder[T](logger, insiClient, objectPrefix, storeType)`
    - **`logger`**: An `*slog.Logger` instance for logging.
    - **`insiClient`**: A configured `*client.Client`.
    - **`objectPrefix`**: A string prefix used for all keys related to this object type (e.g., "users").
    - **`storeType`**: The storage backend to use.
        - `client.StoreTypeValue` (Default): The persistent, disk-backed value store.
        - `client.StoreTypeCache`: The volatile, in-memory cache.
- `WithTransformation(name, fn)`: Registers a `TransformationFunc` with a given name, making it available to the `transform:` tag.

---

## 3. Key Generation Scheme (Under the Hood)

The object manager generates predictable keys to store and retrieve data:

- **Object Field Key**: `[object_prefix]:[primary_key_uuid]:[field_key]`
  - *Example*: `users:a1b2c3d4...:email`
- **Unique Lookup Key**: `[object_prefix]:[field_key]:[field_value]`
  - *Example*: `users:username:johndoe` -> (value is the user's UUID `a1b2c3d4...`)

---

## 4. CRUD & Listing Operations

### Creating Objects

The `New()` method creates and stores a new object. It automatically handles UUID generation, timestamping, and unique constraint enforcement.

```go
newUser := &User{
    Username: "johndoe",
    Email:    "john.doe@example.com",
    PasswordHash: "password123", // This will be transformed by the "hash" function
}

instance, err := userManager.New(context.Background(), newUser)
if err != nil {
    if errors.Is(err, client.ErrUniqueConstraintConflict) {
        // Handle username already exists
    }
    // Handle other errors
}

fmt.Printf("New user created with ID: %s\n", instance.Data().ID)
// The primary key (ID) is automatically populated in the input struct.
fmt.Printf("ID is also set on original struct: %s\n", newUser.ID)
```

**Atomic Operations**: The creation process is designed to be atomic. If a unique constraint fails (e.g., username already exists), the system will automatically roll back and clean up any keys that were created for the failed object.

### Retrieving Objects

You can retrieve an object by its primary key (UUID) or any field marked as `unique`.

```go
// Get by primary key
userInstance, err := userManager.GetByUUID(context.Background(), "a1b2c3d4...")
if err != nil {
    if errors.Is(err, client.ErrObjectNotFound) {
        // Handle not found
    }
    // Handle other errors
}

// Get by a unique field
userInstance, err := userManager.GetByUniqueField(context.Background(), "Username", "johndoe")
if err != nil {
    // Handle errors
}
```

### Deleting Objects

The `Delete()` method removes all data associated with an object, including its field data and all of its unique index entries.

```go
err := userManager.Delete(context.Background(), "a1b2c3d4...")
```

### Listing Objects

The `List()` method retrieves all objects of a given type. It works by scanning the secondary index of a specified `unique` field.

```go
// List all users by discovering them via the unique "Username" field
allUsers, err := userManager.List(context.Background(), "Username")
if err != nil {
    // Handle error
}

for _, userInstance := range allUsers {
    fmt.Printf("Found user: %s\n", userInstance.Data().Username)
}
```
> **Note**: The `List` operation iterates over an index to find all object UUIDs and then fetches each object individually. This can be inefficient for very large datasets and is best suited for smaller collections or administrative tasks.

---

## 5. Granular Field Operations

For performance-critical operations, you can modify single fields without fetching and rewriting the entire object.

- **`SetField(ctx, uuid, fieldName, value)`**: Sets a single field's value.
- **`GetField(ctx, uuid, fieldName)`**: Retrieves a single raw field's value.
- **`CompareAndSwapField(ctx, uuid, fieldName, oldVal, newVal)`**: Performs an atomic Compare-And-Swap operation on a field.

```go
// Atomically update the user's email only if the old value matches
err := userManager.CompareAndSwapField(
    context.Background(),
    userUUID,
    "Email",
    "old.email@example.com",
    "new.email@example.com",
)
```

---

## 6. Manual Index Management

While the system handles most index management automatically during `New()` and `Delete()`, you can manually interact with unique indexes if needed.

- **`SetUniqueNX(ctx, uuid, fieldName, fieldValue)`**: Sets a unique index key only if it does not already exist.
- **`DeleteUnique(ctx, fieldName, fieldValue)`**: Removes a unique index key.
