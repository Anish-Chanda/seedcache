package logger

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

func New(level string) (zerolog.Logger, error) {
	parsed, err := zerolog.ParseLevel(strings.ToLower(strings.TrimSpace(level)))
	if err != nil {
		return zerolog.Logger{}, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	switch parsed {
	case zerolog.TraceLevel, zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel, zerolog.Disabled:
	default:
		return zerolog.Logger{}, fmt.Errorf("invalid log level %q", level)
	}

	console := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
		FormatLevel: func(value any) string {
			name := strings.ToUpper(fmt.Sprint(value))
			if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
				return name
			}
			color := ""
			switch strings.ToLower(name) {
			case "trace":
				color = "34"
			case "debug":
				color = "35"
			case "info":
				color = "32"
			case "warn":
				color = "33"
			case "error", "fatal", "panic":
				color = "31"
			}
			if color == "" {
				return name
			}
			return "\x1b[" + color + "m" + name + "\x1b[0m"
		},
	}
	return zerolog.New(console).With().Timestamp().Logger().Level(parsed), nil
}
