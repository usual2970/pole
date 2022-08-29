package errors

import "errors"

var (
	ErrInvalidUri            = errors.New("invalid URI specified")
	ErrUnSupportedSchemeType = errors.New("unsupported scheme type")
)
