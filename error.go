package disklrucache

type JournalFileFormatError struct {
	msg string
}

func (e *JournalFileFormatError) Error() string {
	return e.msg
}
func NewJournalFileFormatError() *JournalFileFormatError {
	return &JournalFileFormatError{msg: "parse journal file error"}
}
func NewJournalFileFormatErrorWithMsg(msg string) *JournalFileFormatError {
	return &JournalFileFormatError{msg: msg}
}

type JournalVersionError struct {
	msg string
}

func (e *JournalVersionError) Error() string {
	return e.msg
}
func NewJournalVersionError() *JournalVersionError {
	return &JournalVersionError{msg: "journal version error"}
}

type IllegalStateError struct {
	msg string
}

func (e *IllegalStateError) Error() string {
	return e.msg
}
