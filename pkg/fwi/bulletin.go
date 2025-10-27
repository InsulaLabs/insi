package fwi

import "time"

type BulletinBoardControl struct {
	Entity   Entity
	CanRead  bool
	CanWrite bool
}

type BulletinBoardEntry struct {
	Entity Entity
	Entry  string
}

// An asset that is shared amongst multiple entities
type BulletinBoard interface {
	GetOwnerEntity() Entity
	IsOwner(entity Entity) bool

	MaxEntries() int
	CanPost(entity Entity) bool
	CanRead(entity Entity) bool
	CanDelete(entity Entity) bool

	Post(entity Entity, entry BulletinBoardEntry) error
	Read(entity Entity) ([]BulletinBoardEntry, error)

	AddOrModifyControl(
		// Add permissions, update if not already existing
		control BulletinBoardControl,
	) error

	GetPosts(offset, limit int) ([]BulletinBoardEntry, error)
}

// for kv storage
type bulletinBoardEntryDataStore struct {
	EntityUUID string    `json:"entity_uuid"`
	Entry      string    `json:"entry"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type bulletinBoardDataStore struct {
	EntityUUID string                        `json:"entity_uuid"`
	Entries    []bulletinBoardEntryDataStore `json:"entries"`
}
