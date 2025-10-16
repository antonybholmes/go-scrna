import gzip
import sys
import msgpack
import struct

file = sys.argv[1]
index = int(sys.argv[2])


def read_record(filename, index):
    with gzip.open(filename, "rb") as f:
        magic = f.read(4)
        print("Magic:", magic[0])

        # Step 1: Read the offset table entry
        # magic + version + num cells + num entries = 4 + 4 + 4 + 4 = 16 bytes
        f.seek(12)
        num_entries = struct.unpack("<I", f.read(4))[0]

        f.seek(16 + index * 8)
        offset = struct.unpack("<I", f.read(4))[0]

        print(
            "Offset:",
            offset,
        )

        # Step 2: Seek to the start of the record (after header)
        f.seek(16 + num_entries * 8 + offset)

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
