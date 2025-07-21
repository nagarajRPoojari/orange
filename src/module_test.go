package src

import (
	"testing"
)

func TestModuleName(t *testing.T) {
	if ProjectName() != "orange" {
		t.Errorf("Project name `%s` incorrect", ProjectName())
	}
}
