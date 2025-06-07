package tkv

import (
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v3"
)

// newLogger adapts slog.Logger to badger.Logger
type badgerLoggerAdapter struct {
	slogger *slog.Logger
}

func (b *badgerLoggerAdapter) Errorf(format string, args ...interface{}) {
	b.slogger.Error(fmt.Sprintf(format, args...))
}

func (b *badgerLoggerAdapter) Warningf(format string, args ...interface{}) {
	b.slogger.Warn(fmt.Sprintf(format, args...))
}

func (b *badgerLoggerAdapter) Infof(format string, args ...interface{}) {
	b.slogger.Info(fmt.Sprintf(format, args...))
}

func (b *badgerLoggerAdapter) Debugf(format string, args ...interface{}) {
	b.slogger.Debug(fmt.Sprintf(format, args...))
}

func newLogger(slogger *slog.Logger) badger.Logger {
	return &badgerLoggerAdapter{slogger: slogger}
}
