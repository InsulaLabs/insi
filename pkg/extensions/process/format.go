package process

import (
	"fmt"
	"strings"

	processModels "github.com/InsulaLabs/insi/pkg/extensions/process/models"
)

func formatProcessList(processes []processModels.Process, nodeName, nodeID string) string {
	if len(processes) == 0 {
		return fmt.Sprintf("No processes registered on node %s (%s)\n", nodeName, nodeID)
	}

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Node: %s (%s)\n\n", nodeName, nodeID))

	maxNameLen := len("NAME")
	maxUUIDLen := len("UUID")
	for _, p := range processes {
		if len(p.Name) > maxNameLen {
			maxNameLen = len(p.Name)
		}
		if len(p.UUID) > maxUUIDLen {
			maxUUIDLen = len(p.UUID)
		}
	}

	nameWidth := maxNameLen + 2
	uuidWidth := maxUUIDLen + 2
	statusWidth := len("STATUS") + 2

	headerFmt := fmt.Sprintf("%%-%ds %%-%ds %%-%ds\n", nameWidth, uuidWidth, statusWidth)
	rowFmt := fmt.Sprintf("%%-%ds %%-%ds %%-%ds\n", nameWidth, uuidWidth, statusWidth)

	sb.WriteString(fmt.Sprintf(headerFmt, "NAME", "UUID", "STATUS"))
	sb.WriteString(strings.Repeat("-", nameWidth+uuidWidth+statusWidth) + "\n")

	for _, p := range processes {
		sb.WriteString(fmt.Sprintf(rowFmt, p.Name, p.UUID, p.Status))
	}

	sb.WriteString(fmt.Sprintf("\nTotal: %d process(es)\n", len(processes)))

	return sb.String()
}

func formatProcessDetail(p *processModels.Process) string {
	var sb strings.Builder

	sb.WriteString("Process Details:\n")
	sb.WriteString(fmt.Sprintf("  UUID:      %s\n", p.UUID))
	sb.WriteString(fmt.Sprintf("  Name:      %s\n", p.Name))
	sb.WriteString(fmt.Sprintf("  Status:    %s\n", p.Status))
	sb.WriteString(fmt.Sprintf("  Node Name: %s\n", p.NodeName))
	sb.WriteString(fmt.Sprintf("  Node ID:   %s\n", p.NodeID))

	return sb.String()
}

func formatSuccess(message string) string {
	return fmt.Sprintf("✓ %s\n", message)
}

func formatError(message string) string {
	return fmt.Sprintf("✗ %s\n", message)
}

func formatCommandUsage(command string, usage string) string {
	return fmt.Sprintf("Usage: process %s %s\n", command, usage)
}
