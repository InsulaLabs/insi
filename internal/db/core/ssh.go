package core

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/InsulaLabs/insi/internal/app"
	"github.com/InsulaLabs/insi/pkg/fwi"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
	"github.com/charmbracelet/wish/activeterm"
	"github.com/charmbracelet/wish/bubbletea"
	"github.com/charmbracelet/wish/logging"
	"github.com/pkg/errors"
	gossh "golang.org/x/crypto/ssh"
)

const (
	coreSSHEntityKey       = "core_ssh_entity"
	coreEntityIsAdminKey   = "core_ssh_entity_is_admin"
	coreEntityIsAdminValue = "true"
)

func (c *Core) startSSHServer() (err error) {

	srv, err := wish.NewServer(
		wish.WithAddress(net.JoinHostPort(c.nodeCfg.PublicBinding, strconv.Itoa(c.cfg.SSHPort))),
		wish.WithHostKeyPath(c.cfg.HostKeyPath),
		wish.WithPublicKeyAuth(func(ctx ssh.Context, key ssh.PublicKey) bool {
			return c.authenticateUser(ctx, key)
		}),

		ssh.AllocatePty(),

		wish.WithMiddleware(
			bubbletea.Middleware(func(sess ssh.Session) (tea.Model, []tea.ProgramOption) {
				c.logger.Info("New session", "remote_addr", sess.RemoteAddr())
				model, options := c.newSession(sess)
				return model, options
			}),
			activeterm.Middleware(),
			logging.Middleware(),
		),
	)
	if err != nil {
		c.logger.Error("Could not start server", "error", err)
		return err
	}

	go func() {
		c.logger.Info("Starting SSH server", "host", c.nodeCfg.PublicBinding, "port", c.cfg.SSHPort)
		if err = srv.ListenAndServe(); err != nil && !errors.Is(err, ssh.ErrServerClosed) {
			c.logger.Error("Could not start server", "error", err)
		}
	}()

	return nil
}

func (c *Core) authenticateUser(ctx ssh.Context, key ssh.PublicKey) bool {
	if c.fwi == nil {
		c.logger.Error("FWI not initialized for SSH authentication")
		return false
	}

	publicKeyStr := strings.TrimSpace(string(gossh.MarshalAuthorizedKey(key)))

	entity, err := c.fwi.GetEntityByPublicKey(context.Background(), publicKeyStr)
	if err != nil {
		c.logger.Debug("SSH authentication failed: entity not found for public key", "error", err)
		return false
	}

	ctx.SetValue(coreSSHEntityKey, entity)

	isAdmin := false
	for _, adminKey := range c.cfg.AdminSSHKeys {
		if strings.TrimSpace(adminKey) == publicKeyStr {
			isAdmin = true
			break
		}
	}

	if isAdmin {
		ctx.SetValue(coreEntityIsAdminKey, coreEntityIsAdminValue)
	}

	c.logger.Info("SSH user authenticated", "entity", entity.GetName(), "is_admin", isAdmin)
	return true
}

func (c *Core) newSession(sess ssh.Session) (tea.Model, []tea.ProgramOption) {
	entity, ok := sess.Context().Value(coreSSHEntityKey).(fwi.Entity)
	if !ok {
		c.logger.Error("Failed to get entity from SSH context in newSession")
		return nil, nil
	}

	isAdmin := sess.Context().Value(coreEntityIsAdminKey) == coreEntityIsAdminValue

	entityName := entity.GetName()
	if isAdmin {
		entityName = entityName + " (admin)"
	}

	model := app.New(app.ReplConfig{
		SessionConfig: app.SessionConfig{
			Logger:               c.logger.WithGroup("ssh").WithGroup(entity.GetName()),
			UserID:               entity.GetName(),
			ActiveCursorSymbol:   "█",
			InactiveCursorSymbol: " ",
			Prompt:               entityName + " > ",
			FWI:                  c.fwi,
		},
	}, app.AppMap{})

	return model, []tea.ProgramOption{
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	}
}
