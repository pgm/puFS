import time
import os
import sys

def time_walk(root):
    start = time.time()
    count = 0
    for root, dirs, files in os.walk(root):
        count += 1
    end = time.time()
    #print("walked over", count, "files", "in", root)
    assert count > 500
    return end-start

def time_read(filename, read_size):
    start = time.time()
    count = 0
    with open(filename, "rb") as fd:
        for block in iter(lambda: fd.read(read_size), b''):
            count + len(block)
    end = time.time()
    return end-start

def benchmark(root, read_size):
    print("time to walk wide: {:.3e}".format( time_walk(os.path.join(root, "widedir")) ))
    print("time to read 1KB: {:.3e}".format( time_read(os.path.join(root, "1kb"), read_size)))
    print("time to read 1MB: {:.3e}".format( time_read(os.path.join(root, "1mb"), read_size)))
    print("time to read 50MB: {:.3e}".format( time_read(os.path.join(root, "50mb"), read_size)))

if __name__ == "__main__":
    benchmark(sys.argv[1], int(sys.argv[2]))
