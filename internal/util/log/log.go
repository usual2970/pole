package log

var log *ZapLogger

func init() {
	log = NewZapLogger()
	log = log.WithField("service.name", "poled")
}

func WithField(key string, value interface{}) *ZapLogger {
	return log.WithField(key, value)
}

func Debug(msg ...interface{}) {
	log.Debug(msg...)
}

func Info(msg ...interface{}) {
	log.Info(msg...)
}

func Warn(msg ...interface{}) {
	log.Warn(msg...)
}

func Error(msg ...interface{}) {
	log.Error(msg...)
}
