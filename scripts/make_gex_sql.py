import collections
import gzip
import json
import os
import re
import struct
import sys
import msgpack
import pandas as pd
import numpy as np
from nanoid import generate
import uuid_utils as uuid

import argparse

DAT_INDEX_SIZE = 256 * 4
DAT_OFFSET = 1 + 4 + DAT_INDEX_SIZE

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--name", help="name")
parser.add_argument("-i", "--institution", help="institution")
parser.add_argument("-s", "--species", help="species", default="Human")
parser.add_argument("-a", "--assembly", help="assembly", default="GRCh38")
parser.add_argument("-d", "--dir", help="dir")
parser.add_argument("-c", "--cells", help="cells")
parser.add_argument("-l", "--clusters", help="clusters")

args = parser.parse_args()
dir = args.dir
name = args.name
institution = args.institution
species = args.species
assembly = args.assembly
gex_dir = os.path.join(dir, "gex")

public_id = uuid.uuid7()  # = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)

df_cells = pd.read_csv(args.cells, sep="\t", header=0)
df_clusters = pd.read_csv(args.clusters, sep="\t", header=0)

# use cells to count cells in each cluster
counts = []

for c in df_clusters["Cluster"].values:
    count = len(df_cells[df_cells["Cluster"] == c])
    counts.append(count)

df_clusters["Cells"] = counts


with open(os.path.join(dir, "dataset.sql"), "w") as sqlf:

    print(
        f"INSERT INTO dataset (public_id, name, institution, species, assembly, dir) VALUES ('{public_id}', '{name}', '{institution}', '{species}', '{assembly}', '{dir}');",
        file=sqlf,
    )

    print("BEGIN TRANSACTION;", file=sqlf)

    sample_map = {}
    for i, sample in enumerate(sorted(df_cells["Sample"].unique())):
        public_id = uuid.uuid7()  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
        print(
            f"INSERT INTO samples (public_id, name) VALUES ('{public_id}', '{sample}');",
            file=sqlf,
        )
        sample_map[sample] = i + 1

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for i, row in df_clusters.iterrows():
        public_id = uuid.uuid7()
        print(
            f"INSERT INTO clusters (public_id, cluster_id, sc_group, sc_class, cell_count, color) VALUES ('{public_id}', {row["Cluster"]}, '{row["Group"]}', '{row["scClass"]}', {row["Cells"]}, '{row["Color"]}');",
            file=sqlf,
        ),

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for i, row in df_cells.iterrows():
        print(
            f"INSERT INTO cells (barcode, umap_x, umap_y, cluster_id, sample_id) VALUES ('{row["Barcode"]}', {row["UMAP-1"]}, {row["UMAP-2"]}, {row["Cluster"]}, {sample_map[row["Sample"]]});",
            file=sqlf,
        ),

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for f in sorted(os.listdir(gex_dir)):
        # if f.endswith(".json.gz"):
        #     # print(f)
        #     with gzip.open(os.path.join(gex_dir, f), "r") as fin:
        #         data = json.load(fin)

        #         for d in data:
        #             id = d["id"]
        #             sym = d["sym"]

        #             print(
        #                 f"INSERT INTO gex (ensembl_id, gene_symbol, file, offset) VALUES ('{id}', '{sym}', '{f}');",
        #                 file=sqlf,
        #             )

        if f.endswith(".dat"):
            # print(f)
            file = os.path.join(gex_dir, f)
            with open(file, "rb") as fin:

                magic = fin.read(1)
                print("Magic:", file, magic[0])

                # Step 1: Read the offset table entry

                # num genes
                num_entries = struct.unpack("<I", fin.read(4))[0]

                # each entry is 8 bytes (4 bytes offset, 4 bytes size)
                data = fin.read(num_entries * 4 * 2)

                # Unpack as  unsigned ints (little-endian)
                offsets = struct.unpack(f"<{num_entries*2}I", data)

                print(f, num_entries)

                # magic + num_entries + offsets = where msgpack data starts
                dat_offset = 1 + 4 + num_entries * 4 * 2

                for i in range(0, len(offsets), 2):
                    # relative offset for msgpack object
                    offset = offsets[i]

                    # size of msgpack object
                    size = offsets[i + 1]

                    # real offset from start of file
                    seek = dat_offset + offset

                    # skip to the msgpack object
                    fin.seek(seek)

                    # Step 3: Decode one MessagePack object
                    unpacker = msgpack.Unpacker(fin, raw=False)

                    # read the first and only record
                    record = next(unpacker)

                    # now we can get the id and gene symbol
                    id = record["id"]
                    sym = record["sym"]

                    # log the offset and size in the db so we can search
                    # for a gene and then know where to find it in the file
                    print(
                        f"INSERT INTO gex (ensembl_id, gene_symbol, file, offset, size) VALUES ('{id}', '{sym}', '{f}', {seek}, {size});",
                        file=sqlf,
                    )

    print("COMMIT;", file=sqlf)
