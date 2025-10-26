package nerv

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/internal/app"
	"github.com/InsulaLabs/insi/internal/chat"
	"github.com/InsulaLabs/insi/internal/db/core"
	"github.com/InsulaLabs/insi/internal/editor"
	"github.com/InsulaLabs/insi/pkg/client"
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/models"
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

var (
	nervEntityLimits = models.Limits{
		BytesOnDisk:   ptrFrom[int64](10 * 1024 * 1024 * 1024), // 10GB
		BytesInMemory: ptrFrom[int64](10 * 1024 * 1024 * 1024), // 10GB
		EventsEmitted: ptrFrom[int64](1000000),                 // 1 million events
		Subscribers:   ptrFrom[int64](10000),                   // 10 thousand subscribers
	}
)

/*
This here is a tradeoff from how I set the core user isolatiuon up where the limits might be omitted
due to null-possible fields in the representation.

Its technically an artifact from the core database itself regarding entity tracking as we are piggy-backing off
the entity that is used across-cluster where the cluster is of arbitrary size

Architecturally it was easier to just use pointers and permit null
*/
func ptrFrom[R any](v R) *R {
	return &v
}

func (n *Nerv) initializeFWI() {
	endpoints := []client.Endpoint{}
	for _, node := range n.cfg.Nodes {
		endpoints = append(endpoints, client.Endpoint{
			PublicBinding:  node.PublicBinding,
			PrivateBinding: node.PrivateBinding,
			ClientDomain:   node.ClientDomain,
		})
	}

	rootApiKey := sha256.New()
	rootApiKey.Write([]byte(n.cfg.InstanceSecret))
	rootApiKeyHex := hex.EncodeToString(rootApiKey.Sum(nil))
	rootApiKeyBase64 := base64.StdEncoding.EncodeToString([]byte(rootApiKeyHex))

	n.logger.Info("Initializing FWI", "skip_verify", n.cfg.ClientSkipVerify, "endpoints_count", len(endpoints))

	fwi, err := fwi.NewFWI(&client.Config{
		Logger:         n.logger.WithGroup("fwi"),
		ConnectionType: client.ConnectionTypeRandom,
		ApiKey:         rootApiKeyBase64,
		SkipVerify:     n.cfg.ClientSkipVerify,
		Timeout:        time.Second * 30,
		Endpoints:      endpoints,
	}, n.logger.WithGroup("fwi"))

	if err != nil {
		n.logger.Error("Failed to initialize FWI after root keys verified", "error", err)
		return
	}

	n.fwi = fwi
	n.logger.Info("FWI initialized successfully after root keys verified", "skip_verify", n.cfg.ClientSkipVerify)
}

func (n *Nerv) startSSHServer() (err error) {

	if n.nodeCfg.SSHPort == 0 {
		n.logger.Info("SSH port not configured for this node, skipping SSH server")
		return nil
	}

	host, _, err := net.SplitHostPort(n.nodeCfg.PublicBinding)
	if err != nil {
		n.logger.Error("Failed to parse public binding for SSH", "binding", n.nodeCfg.PublicBinding, "error", err)
		return err
	}

	sshAddr := net.JoinHostPort(host, strconv.Itoa(n.nodeCfg.SSHPort))

	srv, err := wish.NewServer(
		wish.WithAddress(sshAddr),
		wish.WithHostKeyPath(n.cfg.HostKeyPath),
		wish.WithPublicKeyAuth(func(ctx ssh.Context, key ssh.PublicKey) bool {
			return n.authenticateUser(ctx, key)
		}),

		ssh.AllocatePty(),

		wish.WithMiddleware(
			bubbletea.Middleware(func(sess ssh.Session) (tea.Model, []tea.ProgramOption) {
				n.logger.Info("New session", "remote_addr", sess.RemoteAddr())
				model, options := n.newSession(sess)
				return model, options
			}),
			activeterm.Middleware(),
			logging.Middleware(),
		),
	)
	if err != nil {
		n.logger.Error("Could not start server", "error", err)
		return err
	}

	go func() {
		n.logger.Info("Starting SSH server", "address", sshAddr)
		if err = srv.ListenAndServe(); err != nil && !errors.Is(err, ssh.ErrServerClosed) {
			n.logger.Error("Could not start server", "error", err)
		}
	}()

	return nil
}

func (n *Nerv) authenticateUser(ctx ssh.Context, key ssh.PublicKey) bool {
	if n.fwi == nil {
		n.logger.Error("FWI not initialized for SSH authentication")
		return false
	}

	publicKeyStr := strings.TrimSpace(string(gossh.MarshalAuthorizedKey(key)))

	isAdmin := false
	for _, adminKey := range n.cfg.AdminSSHKeys {
		if strings.TrimSpace(adminKey) == publicKeyStr {
			isAdmin = true
			break
		}
	}

	if !isAdmin && !n.cfg.EnableNonAdminSSH {
		n.logger.Warn("Non-admin SSH access denied: enableNonAdminSSH is false", "public_key", publicKeyStr)
		return false
	}

	var entity fwi.Entity
	var err error

	aeCtx, aeCancel := context.WithTimeout(context.Background(), time.Second*10)
	defer aeCancel()

	if isAdmin {

		/*
			This entity is for maintaining admin session and configuration information
		*/

		adminEntity, err := n.createAdminEntity(aeCtx)
		if err != nil {
			n.logger.Error("Failed to create admin entity", "error", err)
			return false
		}

		entity = adminEntity

		keys, err := entity.ListPublicKeys(aeCtx)
		if err != nil {
			n.logger.Error("Failed to list public keys for admin entity", "error", err)
		} else {
			found := false
			for _, k := range keys {
				if strings.TrimSpace(k) == publicKeyStr {
					found = true
					break
				}
			}
			if !found {
				if err := entity.AddPublicKey(aeCtx, publicKeyStr); err != nil {
					n.logger.Warn("Failed to add admin public key to entity", "error", err)
				} else {
					n.logger.Info("Added admin public key to admin entity")
				}
			}
		}

		ctx.SetValue(coreEntityIsAdminKey, coreEntityIsAdminValue)
	} else {
		entity, err = n.fwi.GetEntityByPublicKey(aeCtx, publicKeyStr)
		if err != nil {
			n.logger.Debug("SSH authentication failed: entity not found for public key", "error", err)
			return false
		}
	}

	ctx.SetValue(coreSSHEntityKey, entity)

	n.logger.Info("SSH user authenticated", "entity", entity.GetName(), "is_admin", isAdmin)
	return true
}

func (n *Nerv) newSession(sess ssh.Session) (tea.Model, []tea.ProgramOption) {
	entity, ok := sess.Context().Value(coreSSHEntityKey).(fwi.Entity)
	if !ok {
		n.logger.Error("Failed to get entity from SSH context in newSession")
		return nil, nil
	}

	isAdmin := sess.Context().Value(coreEntityIsAdminKey) == coreEntityIsAdminValue

	/*
		We authenticate based off of an entity public key.
		Given that we pull entity this way we may or may not
		be an administrator (if they setup a new entity with a root FWI)
	*/

	entityName := entity.GetName()
	if isAdmin {
		entityName = entityName + " (admin)"
	} else {
		entityName = entityName + " (user)"
	}

	extensionControls := make([]core.ExtensionControl, len(n.extensions))
	for i, extension := range n.extensions {
		extensionControls[i] = extension.GetContorller()
	}

	entityFS := entity.GetFS()

	model := app.New(n.ctx, app.ReplConfig{
		SessionConfig: app.SessionConfig{
			Logger:               n.logger.WithGroup("ssh").WithGroup(entity.GetName()),
			UserID:               entity.GetName(),
			ActiveCursorSymbol:   "█",
			InactiveCursorSymbol: " ",
			Prompt:               entityName + " > ",
			UserFWI:              entity,
		},
	}, buildAppMap(entityFS), extensionControls)

	return model, []tea.ProgramOption{}
}

func buildAppMap(fs fwi.FS) app.AppMap {

	chatApp, chatAppConstructor := chat.AppEntry()
	editorApp, editorAppConstructor := editor.AppEntry()

	editor.SetFS(fs)

	return app.AppMap{
		chatApp:   chatAppConstructor,
		editorApp: editorAppConstructor,
	}
}
