package core

import (
	"net"
	"strconv"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/ssh"
	"github.com/charmbracelet/wish"
	"github.com/charmbracelet/wish/activeterm"
	"github.com/charmbracelet/wish/bubbletea"
	"github.com/charmbracelet/wish/logging"
	"github.com/pkg/errors"
)

const (
	coreSSHEntityKey       = "core_ssh_entity"
	coreEntityIsAdminKey   = "core_ssh_entity_is_admin"
	coreEntityIsAdminValue = "true" // if the entity is using a server root "admin" key
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
	// TODO
	/*
		We need to use the cores FWI interface to find the Entity by key
		If not exist, return false

		If Exist, true and we need to stuff the entity into the ssh CTX so it can
		be utilized by the handlerss at coreSSHEntity key (see above)

		Check if entity is an admin by comparing it against the Core cluster config's admin keys list
		and store "true" or "false" so the session can easily determine if the entity is admin
	*/
	return false
}

func (c *Core) newSession(sess ssh.Session) (tea.Model, []tea.ProgramOption) {
	// TODO
	/*
		here we will construct the actual ssh model
	*/
	return nil, nil
}
