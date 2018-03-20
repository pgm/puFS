package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecuteScript(t *testing.T) {
	require := require.New(t)

	e, err := executeScript("d a\nw a/b")
	require.Nil(err)

	require.Equal(0, e.errorCount)
}
