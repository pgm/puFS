package main

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

const MaxReadLen = 1024 * 1024 * 3

func compareFile(r *rand.Rand, fileA, fileB string) int64 {
	bytesCompared := int64(0)

	st, err := os.Stat(fileA)
	if err != nil {
		panic(err)
	}
	filesize := st.Size()

	fA, err := os.Open(fileA)
	if err != nil {
		panic(err)
	}
	defer fA.Close()

	fB, err := os.Open(fileB)
	if err != nil {
		panic(err)
	}
	defer fB.Close()

	fullBufA := make([]byte, MaxReadLen)
	fullBufB := make([]byte, MaxReadLen)
	seqReads := r.Intn(1) == 0
	readCount := 5
	//readLen := int64(r.Intn(MaxReadLen) + 1)

	readLen := int64(math.Exp(r.Float64()*math.Log(float64(MaxReadLen)))) + 1
	//	log.Printf("readLen=%d", readLen)

	offset := r.Int63n(filesize - int64(readCount)*readLen)
	if seqReads {
		_, err = fA.Seek(offset, os.SEEK_SET)
		if err != nil {
			panic(err)
		}
		_, err = fB.Seek(offset, os.SEEK_SET)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < readCount; i++ {
		bufA := fullBufA[:readLen]
		bufB := fullBufB[:readLen]

		if seqReads {
			n, err := fA.Read(bufA)
			if err != nil {
				panic(err)
			}
			if int64(n) != readLen {
				log.Fatalf("Expected read @%d to return %d bytes but returned %d", offset, readLen, int64(n))
			}

			n, err = fB.Read(bufB)
			if err != nil {
				panic(err)
			}
			if int64(n) != readLen {
				log.Fatalf("Expected read @%d to return %d bytes but returned %d", offset, readLen, int64(n))
			}

			if bytes.Compare(bufA, bufB) != 0 {
				panic("mismatch")
			}
			bytesCompared += int64(len(bufA))
			offset += int64(n)
		} else {
			offset := r.Int63n(filesize - readLen)
			n, err := fA.ReadAt(bufA, offset)
			if err != nil {
				panic(err)
			}
			if int64(n) != readLen {
				log.Fatalf("Expected read @%d to return %d bytes but returned %d", offset, readLen, int64(n))
			}

			offset = r.Int63n(filesize - readLen)
			n, err = fB.ReadAt(bufB, offset)
			if err != nil {
				panic(err)
			}
			if int64(n) != readLen {
				log.Fatalf("Expected read @%d to return %d bytes but returned %d", offset, readLen, int64(n))
			}

			if bytes.Compare(bufA, bufB) != 0 {
				panic("mismatch")
			}
			bytesCompared += int64(len(bufA))
		}
	}

	return bytesCompared
}

func compareFiles(r *rand.Rand, pathA string, pathB string, files []string, duration time.Duration) int64 {
	start := time.Now()
	runEnd := start.Add(duration)
	bytesCompared := int64(0)
	for time.Now().Before(runEnd) {
		file := files[r.Intn(len(files))]
		fileA := path.Join(pathA, file)
		fileB := path.Join(pathB, file)
		bytesCompared += compareFile(r, fileA, fileB)
	}
	return bytesCompared
}

func main() {
	args := os.Args[1:]
	if len(args) < 4 {
		log.Fatalf("Not enough arguments. Required: <concurrency count> <tree a> <tree b> <file 1> ... <file n>")
	}
	threads_, err := strconv.ParseInt(args[0], 10, 32)
	if err != nil {
		panic(err)
	}
	threads := int(threads_)
	treeA := args[1]
	treeB := args[2]
	files := args[3:]

	var barrier sync.WaitGroup
	barrier.Add(threads)

	r := rand.New(rand.NewSource(99))
	for i := 0; i < threads; i++ {
		go (func(i int) {
			log.Printf("Starting %d", i)
			bytesCompared := compareFiles(r, treeA, treeB, files, 10*time.Second)
			log.Printf("Finished %d: %d bytesCompared", i, bytesCompared)
			barrier.Done()
		})(i)
	}

	barrier.Wait()
}
