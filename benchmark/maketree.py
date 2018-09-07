import os
import sys

KB = 1024
MB = 1024 * KB

def write_file(fn, size):
    with open(fn, "wb") as fd:
        fd.write(bytearray(size))

def write_tree(root, levels, dir_count, child_files_count, file_size):
    if levels == 0:
        for i in range(child_files_count):
            file_name = os.path.join(root, str(i))
            os.makedirs(os.path.dirname(file_name))
            write_file(file_name, file_size)
    else:
        for i in range(dir_count):
            write_tree(os.path.join(root, str(i)), levels - 1, dir_count, child_files_count, file_size)

def create_all(root):
    write_tree(os.path.join(root, "widedir"), 1, 500, 1, 4*KB)
    write_file(os.path.join(root, "1kb"), 1 * MB)
    write_file(os.path.join(root, "1mb"), 1 * MB)
    write_file(os.path.join(root, "50mb"), 50 * MB)

if __name__ == "__main__":
    create_all(sys.argv[1])
