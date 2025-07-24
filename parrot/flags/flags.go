package flags

type Flag int

const (
	KeyDeletedFlag  Flag = 1
	KeyNotFoundFlag Flag = 2
	KeyFoundFlag    Flag = 0
)
