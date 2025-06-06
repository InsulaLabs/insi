package status

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/runtime"
)

type StatusPlugin struct {
	logger *slog.Logger
	prif   runtime.ServiceRuntimeIF

	startedAt time.Time
}

var _ runtime.Service = &StatusPlugin{}

func New(logger *slog.Logger) *StatusPlugin {
	return &StatusPlugin{
		logger: logger,
	}
}

func (p *StatusPlugin) GetName() string {
	return "status"
}

func (p *StatusPlugin) Init(prif runtime.ServiceRuntimeIF) *runtime.ServiceImplError {
	p.prif = prif
	p.startedAt = time.Now()
	return nil
}

func (p *StatusPlugin) GetRoutes() []runtime.ServiceRoute {
	return []runtime.ServiceRoute{
		/*
			IMPORTANT:
				DO NOT PREFIX THE PATH WITH TEH NAME OF THE PLUGIN

				THIS IS OF PARAMOUNT IMPORTANCE AS THE ROUTES ARE AUTOMATICALLY MOUNTED
				TO THE SERVICE PREFIXED BY THE NAME OF THE PLUGIN.
				ADDING "/service-name" HERE WILL CASUSE "/service-name/service-name/uptime"
				TO BE MOUNTED.
		*/
		{Path: "uptime", Handler: http.HandlerFunc(p.uptimeHandler), Limit: 10, Burst: 10},
	}
}

// / -------- routes --------
func (p *StatusPlugin) uptimeHandler(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	uptime := now.Sub(p.startedAt)

	// Set cache busting headers
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("Content-Type", "application/json")

	w.Write(
		[]byte(
			fmt.Sprintf(
				`{"status":"OK","startedAt":"%s","uptime":"%s"}`,
				p.startedAt.Format(time.RFC3339),
				uptime.String(),
			),
		),
	)
	w.Write([]byte("\n"))
	w.WriteHeader(http.StatusOK)
}
