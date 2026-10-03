package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/quyenhl16/cap-mesh/internal/adapter/clientconfig"
)

func translateCommandArgs(args []string) ([]string, bool, error) {
	prefix, commandArgs := leadingGlobalArgs(args)
	if len(prefix) > 0 {
		if len(commandArgs) == 0 || strings.HasPrefix(commandArgs[0], "-") {
			return args, false, nil
		}
		mapped, modern, err := translateCommandArgs(commandArgs)
		if err != nil || !modern {
			return mapped, modern, err
		}
		return append(mapped, prefix...), true, nil
	}
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return args, false, nil
	}
	verb := strings.ToLower(args[0])
	if verb == "help" {
		return []string{"--help"}, true, nil
	}
	if verb == "config" {
		return args, true, nil
	}
	if len(args) < 2 {
		return nil, true, fmt.Errorf("%s requires a resource", verb)
	}
	resource := normalizeResource(args[1])
	rest := args[2:]
	withName := func(flagName string) ([]string, error) {
		if len(rest) == 0 || strings.HasPrefix(rest[0], "-") {
			return nil, fmt.Errorf("%s %s requires a name", verb, resource)
		}
		return append([]string{flagName, rest[0]}, rest[1:]...), nil
	}
	var mapped []string
	switch verb + "/" + resource {
	case "get/sessions":
		mapped = append([]string{"--list-sessions"}, rest...)
	case "get/session", "describe/session":
		value, err := withName("--get-session")
		return value, true, err
	case "get/agents":
		mapped = append([]string{"--list-agents"}, rest...)
	case "get/agent", "describe/agent":
		value, err := withName("--get-agent")
		return value, true, err
	case "get/recordings":
		mapped = append([]string{"--list-recordings"}, rest...)
	case "get/recording", "describe/recording":
		value, err := withName("--get-recording")
		return value, true, err
	case "get/continuous", "describe/continuous":
		mapped = append([]string{"--continuous-status"}, rest...)
	case "get/logcapture", "describe/logcapture":
		mapped = append([]string{"--log-status"}, rest...)
	case "create/session":
		mapped = append([]string{"--create-only"}, rest...)
	case "capture/session":
		mapped = append([]string{"--create"}, rest...)
	case "stream/session":
		value, err := withName("--session")
		return value, true, err
	case "stop/session":
		value, err := withName("--stop-session")
		return value, true, err
	case "start/continuous":
		mapped = append([]string{"--continuous-start"}, rest...)
	case "stop/continuous":
		mapped = append([]string{"--continuous-stop"}, rest...)
	case "start/logcapture":
		mapped = append([]string{"--log-start"}, rest...)
	case "stop/logcapture":
		mapped = append([]string{"--log-stop"}, rest...)
	case "clean/recordings":
		mapped = append([]string{"--clean-recordings"}, rest...)
	default:
		return nil, true, fmt.Errorf("unsupported command %q", strings.Join(args[:2], " "))
	}
	return mapped, true, nil
}

func leadingGlobalArgs(args []string) ([]string, []string) {
	valueFlags := map[string]bool{
		"--config": true, "--context": true, "--server": true, "--token": true,
		"--token-file": true, "--tls-ca": true, "--tls-server-name": true,
	}
	var prefix []string
	for len(args) > 0 {
		name := args[0]
		if name == "--insecure" || strings.HasPrefix(name, "--insecure=") {
			prefix = append(prefix, name)
			args = args[1:]
			continue
		}
		base := name
		if index := strings.IndexByte(name, '='); index >= 0 {
			base = name[:index]
		}
		if !valueFlags[base] {
			break
		}
		prefix = append(prefix, name)
		args = args[1:]
		if base == name {
			if len(args) == 0 {
				return prefix, args
			}
			prefix = append(prefix, args[0])
			args = args[1:]
		}
	}
	return prefix, args
}

func normalizeResource(value string) string {
	switch strings.ToLower(value) {
	case "sessions":
		return "sessions"
	case "session":
		return "session"
	case "agents":
		return "agents"
	case "agent":
		return "agent"
	case "recordings":
		return "recordings"
	case "recording":
		return "recording"
	case "continuous":
		return "continuous"
	case "log", "logs", "logcapture", "logcaptures":
		return "logcapture"
	default:
		return strings.ToLower(value)
	}
}

func argumentValue(args []string, name, fallback string) string {
	for index := 0; index < len(args); index++ {
		if args[index] == name && index+1 < len(args) {
			return args[index+1]
		}
		if strings.HasPrefix(args[index], name+"=") {
			return strings.TrimPrefix(args[index], name+"=")
		}
	}
	return fallback
}

func printCommandUsage(output io.Writer) {
	fmt.Fprintln(output, `Usage:
  capmesh-client get sessions|agents|recordings|continuous|logcapture
  capmesh-client get session|agent|recording NAME
  capmesh-client describe session|agent|recording NAME
  capmesh-client create session [flags]
  capmesh-client capture session [flags]
  capmesh-client stream session NAME
  capmesh-client stop session NAME
  capmesh-client start|stop continuous [flags]
  capmesh-client start|stop logcapture [flags]
  capmesh-client clean recordings [--dry-run] [--yes]
  capmesh-client config view|get-contexts|current-context|use-context|set-context

Global connection flags override the selected context: --config, --context,
--server, --token, --token-file, --tls-ca, --tls-server-name, --insecure.`)
}

func runConfigCommand(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("config requires a subcommand")
	}
	subcommand := args[0]
	commandArgs := args[1:]
	defaultPath, err := clientconfig.DefaultPath()
	if err != nil {
		return err
	}
	switch subcommand {
	case "view":
		flags := flag.NewFlagSet("config view", flag.ContinueOnError)
		flags.SetOutput(stderr)
		path := flags.String("config", defaultPath, "client config file")
		contextName := flags.String("context", "", "context override")
		effective := flags.Bool("effective", false, "show resolved context and environment values")
		if err := flags.Parse(commandArgs); err != nil {
			return err
		}
		config, err := clientconfig.Load(*path)
		if err != nil {
			return err
		}
		var output any = config
		if *effective {
			resolved, err := config.Resolve(*contextName, os.LookupEnv)
			if err != nil {
				return err
			}
			resolved.Token = redact(resolved.Token)
			output = resolved
		}
		data, err := clientconfig.Marshal(output)
		if err != nil {
			return err
		}
		_, err = stdout.Write(data)
		return err
	case "get-contexts":
		flags := flag.NewFlagSet("config get-contexts", flag.ContinueOnError)
		flags.SetOutput(stderr)
		path := flags.String("config", defaultPath, "client config file")
		if err := flags.Parse(commandArgs); err != nil {
			return err
		}
		config, err := clientconfig.Load(*path)
		if err != nil {
			return err
		}
		for _, name := range config.ContextNames() {
			marker := " "
			if name == config.CurrentContext {
				marker = "*"
			}
			fmt.Fprintf(stdout, "%s %s\n", marker, name)
		}
		return nil
	case "current-context":
		flags := flag.NewFlagSet("config current-context", flag.ContinueOnError)
		flags.SetOutput(stderr)
		path := flags.String("config", defaultPath, "client config file")
		if err := flags.Parse(commandArgs); err != nil {
			return err
		}
		config, err := clientconfig.Load(*path)
		if err != nil {
			return err
		}
		if config.CurrentContext == "" {
			return fmt.Errorf("current context is not configured")
		}
		fmt.Fprintln(stdout, config.CurrentContext)
		return nil
	case "use-context":
		flags := flag.NewFlagSet("config use-context", flag.ContinueOnError)
		flags.SetOutput(stderr)
		path := flags.String("config", defaultPath, "client config file")
		name, parseArgs := leadingName(commandArgs)
		if err := flags.Parse(parseArgs); err != nil {
			return err
		}
		if name == "" && flags.NArg() == 1 {
			name = flags.Arg(0)
		}
		if name == "" || flags.NArg() > 1 {
			return fmt.Errorf("config use-context requires one context name")
		}
		config, err := clientconfig.Load(*path)
		if err != nil {
			return err
		}
		if _, ok := config.Contexts[name]; !ok {
			return fmt.Errorf("context %q does not exist", name)
		}
		config.CurrentContext = name
		if err := clientconfig.Save(*path, config); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Switched to context %q.\n", name)
		return nil
	case "set-context":
		return setContext(commandArgs, defaultPath, stdout, stderr)
	default:
		return fmt.Errorf("unsupported config subcommand %q", subcommand)
	}
}

func setContext(args []string, defaultPath string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("config set-context", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", defaultPath, "client config file")
	server := flags.String("server", "", "capmesh-server address")
	insecure := flags.Bool("insecure", false, "disable TLS")
	tlsCA := flags.String("tls-ca", "", "server CA certificate")
	tlsServerName := flags.String("tls-server-name", "", "TLS server name")
	tokenFile := flags.String("token-file", "", "bearer token file")
	snaplen := flags.Uint("snaplen", 0, "default packet snapshot length")
	ttl := flags.String("ttl", "", "default session lifetime")
	reorderWindow := flags.String("reorder-window", "", "default packet reorder window")
	direction := flags.String("direction", "", "default workload direction")
	follow := flags.Bool("follow", true, "default workload follow behavior")
	maxPods := flags.Uint("max-pods", 0, "default workload pod limit")
	name, parseArgs := leadingName(args)
	if err := flags.Parse(parseArgs); err != nil {
		return err
	}
	if name == "" && flags.NArg() == 1 {
		name = flags.Arg(0)
	}
	if name == "" || flags.NArg() > 1 {
		return fmt.Errorf("config set-context requires one context name")
	}
	config, err := clientconfig.Load(*path)
	if err != nil {
		return err
	}
	selected := config.Contexts[name]
	visited := make(map[string]bool)
	flags.Visit(func(item *flag.Flag) { visited[item.Name] = true })
	if visited["server"] {
		selected.Server = *server
	}
	if visited["insecure"] {
		selected.Insecure = *insecure
	}
	if visited["tls-ca"] {
		selected.TLSCA = *tlsCA
	}
	if visited["tls-server-name"] {
		selected.TLSServerName = *tlsServerName
	}
	if visited["token-file"] {
		selected.TokenFile = *tokenFile
	}
	if visited["snaplen"] {
		selected.Defaults.Snaplen = *snaplen
	}
	if visited["ttl"] {
		selected.Defaults.TTL = *ttl
	}
	if visited["reorder-window"] {
		selected.Defaults.ReorderWindow = *reorderWindow
	}
	if visited["direction"] {
		selected.Defaults.Direction = *direction
	}
	if visited["follow"] {
		selected.Defaults.Follow = follow
	}
	if visited["max-pods"] {
		selected.Defaults.MaxPods = *maxPods
	}
	config.Contexts[name] = selected
	if config.CurrentContext == "" {
		config.CurrentContext = name
	}
	if err := clientconfig.Save(*path, config); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Context %q updated.\n", name)
	return nil
}

func redact(value string) string {
	if value == "" {
		return ""
	}
	return "REDACTED"
}

func leadingName(args []string) (string, []string) {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		return args[0], args[1:]
	}
	return "", args
}
