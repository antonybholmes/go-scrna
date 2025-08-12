import gzip
import sys
import msgpack
import struct

file = sys.argv[1]
index = int(sys.argv[2])


def read_record(filename, index):
    with gzip.open(filename, "rb") as f:
        magic = f.read(1)
        print("Magic:", magic[0])

        # Step 1: Read the offset table entry
        f.seek(1 + index * 4)
        offset = struct.unpack("<I", f.read(4))[0]

        print("Offset:", offset, 1 + 256 * 4 + offset)

        # Step 2: Seek to the start of the record (after header)
        f.seek(1 + 256 * 4 + offset)

        # Step 3: Decode one MessagePack object
        unpacker = msgpack.Unpacker(f, raw=False)
        return next(unpacker)


def read_offset(filename, offset):
    with open(filename, "rb") as f:

        # Step 1: Read the offset table entry
        f.seek(offset)
        # offset = struct.unpack("<I", f.read(4))[0]

        print("Offset:", offset)

        # Step 2: Seek to the start of the record (after header)
        f.seek(offset)

        # Step 3: Decode one MessagePack object
        unpacker = msgpack.Unpacker(f, raw=False)
        return next(unpacker)


# r = read_record(file, index)
r = read_offset(file, index)
print(r)
