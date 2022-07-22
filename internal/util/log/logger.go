package log

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"pole/internal/util/runenv"
)

var path = "/data/log/sq"

type ZapLogger struct {
	log  *zap.Logger
	Sync func() error
	kvs  []zapcore.Field
}

// NewZapLogger return ZapLogger
func NewZapLogger() *ZapLogger {
	now := time.Now()
	hook := lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/%04d%02d%02d.log", path, now.Year(), now.Month(), now.Day()), // filePath
		MaxSize:    128,
		MaxAge:     14,
		MaxBackups: 30,
		Compress:   false,
	}
	writer := zapcore.AddSync(os.Stdout)
	if !runenv.IsDev() {
		writer = zapcore.AddSync(&hook)
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.NewMultiWriteSyncer(
			writer,
		), zap.InfoLevel)
	zapLogger := zap.New(core, zap.WithCaller(true), zap.AddCallerSkip(1))
	return &ZapLogger{log: zapLogger, Sync: zapLogger.Sync, kvs: make([]zap.Field, 0)}
}

func (l *ZapLogger) WithField(key string, value interface{}) *ZapLogger {
	clone := l.clone()
	clone.kvs = append(clone.kvs, zap.Any(key, value))
	return clone
}

func (l *ZapLogger) clone() *ZapLogger {
	var kvs []zap.Field
	kvs = append(kvs, l.kvs...)
	return &ZapLogger{
		kvs:  kvs,
		log:  l.log,
		Sync: l.Sync,
	}
}

func (l *ZapLogger) Debug(msg ...interface{}) {
	l.log.Debug(fmt.Sprint(msg...), l.kvs...)
}

func (l *ZapLogger) Info(msg ...interface{}) {
	l.log.Info(fmt.Sprint(msg...), l.kvs...)
}

func (l *ZapLogger) Warn(msg ...interface{}) {
	l.log.Warn(fmt.Sprint(msg...), l.kvs...)
}

func (l *ZapLogger) Error(msg ...interface{}) {
	l.log.Error(fmt.Sprint(msg...), l.kvs...)
}
