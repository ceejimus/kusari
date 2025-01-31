package logger

import (
	"log"
	"os"
	"runtime"
	"strings"
)

type LogLevel int

const (
	TRACE LogLevel = iota
	DEBUG
	INFO
	WARN
	ERROR
	FATAL
)

const DEFAULT_LOG_LEVEL = 2

var logLevelNames = []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

var logger Logger

func (level LogLevel) String() string {
	if level < TRACE || level > ERROR {
		return logLevelNames[DEFAULT_LOG_LEVEL]
	}
	return logLevelNames[level]
}

type Logger struct {
	level LogLevel
}

func (Logger) ParseLogLevel(levelStr string) LogLevel {
	for i, name := range logLevelNames {
		if strings.EqualFold(levelStr, name) {
			return LogLevel(i)
		}
	}

	return -1
}

func (logger *Logger) log(level LogLevel, message string) {
	if level < logger.level {
		return
	}

	pc, _, _, ok := runtime.Caller(2)
	funcName := "unknown"
	if ok {
		fullFuncName := runtime.FuncForPC(pc).Name()
		parts := strings.Split(fullFuncName, "/")
		funcName = parts[len(parts)-1]
	}

	log.Printf("[%s] %s: %s", level, funcName, message)
}

func Trace(message string) {
	logger.log(TRACE, message)
}

func Debug(message string) {
	logger.log(DEBUG, message)
}

func Info(message string) {
	logger.log(INFO, message)
}

func Warn(message string) {
	logger.log(WARN, message)
}

func Error(message string) {
	logger.log(ERROR, message)
}

func Fatal(message string) {
	logger.log(ERROR, message)
}

func Init(levelStr string) {
	// override w/ env variable
	envLevelStr := os.Getenv("FILESERVER_LOG_LEVEL")
	if envLevelStr != "" {
		levelStr = envLevelStr
	}
	level := Logger{}.ParseLogLevel(levelStr)
	logger = Logger{level: level}
}
