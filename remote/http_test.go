package remote

// func TestURLRemote(t *testing.T) {
// 	require := require.New(t)

// 	gob.Register(core.URLSource{})
// 	url := "https://developer.mozilla.org/en-US/"
// 	d := testDataStore()
// 	d.SetClients(&NetworkClientImp{})

// 	ctx := context.Background()

// 	fileID, err := d.AddRemoteURL(ctx, RootINode, "remote", url)
// 	require.Nil(err)

// 	r, err := d.GetReadRef(ctx, fileID)
// 	require.Nil(err)

// 	buffer := make([]byte, 500)
// 	_, err = r.Read(ctx, buffer)
// 	require.Nil(err)

// 	require.Equal("HTML", string(buffer))
// 	fmt.Printf("%s", string(buffer))
// }
