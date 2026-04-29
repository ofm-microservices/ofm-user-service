package repository

// DBErrorTranslator maps storage-driver failures into domain-aware repository
// errors.
type DBErrorTranslator interface {
	TranslateCreateUserError(err error) error
	TranslateFindUserError(err error) error
	TranslateDeleteUserError(err error) error
}
