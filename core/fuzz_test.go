// +build broken_test

package core

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecuteScript(t *testing.T) {
	require := require.New(t)

	e, err := executeScript("d a\nw a/b")
	require.Nil(err)

	require.Equal(0, e.errorCount)
}

// func TestRunCrash1(t *testing.T) {
// 	require := require.New(t)
// 	script := "d a\nd a/b\nu a/b\np a 0"

// 	e, err := executeScript(script)
// 	require.Nil(err)
// 	require.Equal(0, e.errorCount)
// }

func TestRunCorpusSamples(t *testing.T) {
	require := require.New(t)
	prefix := "/Users/pmontgom/go/src/github.com/pgm/sply2/examples/corpus/"
	filenames := []string{"basic", "create-remove", "deep-tree-freeze", "push-pull"}
	for _, filename := range filenames {
		fmt.Printf("Running %s...\n", filename)
		f, err := os.Open(prefix + filename)
		require.Nil(err)

		b, err := ioutil.ReadAll(f)
		require.Nil(err)
		f.Close()

		e, err := executeScript(string(b))
		require.Nil(err)

		require.Equal(0, e.errorCount)

	}
}
