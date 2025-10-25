package extensions

import "github.com/InsulaLabs/insi/internal/db/core"

type InsiModule interface {
	Name() string
	Version() string
	Description() string
	core.Extension
}
