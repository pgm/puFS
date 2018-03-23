package sply2

// func testINodeDB() *INodeDB {
// 	dir, err := ioutil.TempDir("", "test")
// 	fmt.Printf("dir=%s\n", dir)
// 	if err != nil {
// 		panic(err)
// 	}
// 	ds := NewINodeDB(dir+"/db", 100)
// 	return ds
// }

// func TestNodeDbRW(t *testing.T) {
// 	require := require.New(t)
// 	d := testINodeDB()

// 	d.update(func(tx RTx) error {
// 		fmt.Println("Before")
// 		printDbStats(tx)

// 		_, err := d.AddDir(tx, RootINode, "a")

// 		require.Nil(err)
// 		fmt.Println("After")
// 		printDbStats(tx)

// 		return nil
// 	})

// 	d.view(func(tx RTx) error {
// 		names, err := d.GetDirContents(tx, RootINode)
// 		require.Nil(err)
// 		require.EqualValues([]string{"a"}, names)

// 		node, err := d.GetNode(tx, RootINode, "a")
// 		require.Nil(err)
// 		require.True(node.IsDir)

// 		return nil
// 	})
// }
