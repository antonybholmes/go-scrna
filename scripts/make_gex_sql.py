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
from pysam import index
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


df_cells = pd.read_csv(args.cells, sep="\t", header=0)
df_clusters = pd.read_csv(args.clusters, sep="\t", header=0, index_col=0)

# get rid of clusters 101 etc
df_cells = df_cells[df_cells["Cluster"].isin(df_clusters.index)]

print(df_clusters)


# use cells to count cells in each cluster
counts = []

for c in df_clusters.index:
    count = len(df_cells[df_cells["Cluster"] == c])
    counts.append(count)

print(counts)

cluster_id_map = {c: uuid.uuid7() for i, c in enumerate(df_clusters.index)}

# df_clusters["Cells"] = counts

metadata_types = list(sorted(df_clusters.columns[1:].values))

metadata_type_map = {name: uuid.uuid7() for name in metadata_types}

with open(os.path.join(dir, "dataset.sql"), "w") as sqlf:
    dataset_id = uuid.uuid7()  # = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)

    print(
        f"INSERT INTO dataset (id, name, institution, species, assembly, cells, dir) VALUES ('{dataset_id}', '{name}', '{institution}', '{species}', '{assembly}', {df_cells.shape[0]}, '{dir}');",
        file=sqlf,
    )

    print("BEGIN TRANSACTION;", file=sqlf)

    sample_map = {}
    for i, sample in enumerate(sorted(df_cells["Sample"].unique())):
        sample_id = uuid.uuid7()  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
        print(
            f"INSERT INTO samples (id, dataset_id, name) VALUES ('{sample_id}', '{dataset_id}', '{sample}');",
            file=sqlf,
        )
        sample_map[sample] = sample_id  # i + 1

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for idx, (cluster, row) in enumerate(df_clusters.iterrows()):
        cluster_id = cluster_id_map[row.name]

        print(
            f"INSERT INTO clusters (id, name, cell_count, color) VALUES ('{cluster_id}', '{cluster}',  {counts[idx]}, '{row["Color"]}');",
            file=sqlf,
        )

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for name in metadata_types:
        metadata_id = metadata_type_map[name]  # uuid.uuid7()
        print(
            f"INSERT INTO metadata_types (id, name) VALUES ('{metadata_id}', '{name}');",
            file=sqlf,
        )

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    metadata_map = collections.defaultdict(lambda: {})

    for i, name in enumerate(metadata_types):
        metadata_type_id = metadata_type_map[name]
        for v in sorted(df_clusters[name].unique()):
            metadata_id = uuid.uuid7()
            print(
                f"INSERT INTO metadata (id, metadata_type_id, value) VALUES ('{metadata_id}', '{metadata_type_id}',  '{v}');",
                file=sqlf,
            )
            metadata_map[name][v] = metadata_id  # index

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    print(metadata_map, metadata_types)

    # clusters can have metadata attached to them
    for idx, (i, row) in enumerate(df_clusters.iterrows()):
        cluster_id = cluster_id_map[row.name]
        for j, metadata_type in enumerate(metadata_types):
            cluster_metadata_id = uuid.uuid7()
            metadata_value = row[j + 1]
            print(metadata_type, metadata_value)
            metadata_id = metadata_map[metadata_type][metadata_value]
            print(
                f"INSERT INTO cluster_metadata (id, cluster_id, metadata_id) VALUES ('{cluster_metadata_id}', '{cluster_id}', '{metadata_id}');",
                file=sqlf,
            )

    print("COMMIT;", file=sqlf)

    print("BEGIN TRANSACTION;", file=sqlf)

    for i, row in df_cells.iterrows():
        cell_id = uuid.uuid7()
        print(
            f"INSERT INTO cells (id, cluster_id, sample_id, barcode, umap_x, umap_y) VALUES ('{cell_id}', '{cluster_id_map[row["Cluster"]]}', '{sample_map[row["Sample"]]}', '{row["Barcode"]}', {row["UMAP-1"]}, {row["UMAP-2"]});",
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

                magic = fin.read(4)
                print("Magic:", file, magic[0])

                # Step 1: Read the offset table entry

                version = struct.unpack("<I", fin.read(4))[0]
                print("Version:", version)
                cells = struct.unpack("<I", fin.read(4))[0]
                print("Cells:", cells)

                # num genes
                num_entries = struct.unpack("<I", fin.read(4))[0]

                # each entry is 8 bytes (4 bytes offset, 4 bytes size)
                data = fin.read(num_entries * 4 * 2)

                # Unpack as  unsigned ints (little-endian)
                offsets = struct.unpack(f"<{num_entries*2}I", data)

                print(f, num_entries)

                # magic + num_entries + offsets = where msgpack data starts
                dat_offset = 4 + 4 + 4 + 4 + num_entries * 4 * 2

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
                    gex_id = uuid.uuid7()
                    ensembl_id = record["id"]
                    gene_symbol = record["s"]

                    # log the offset and size in the db so we can search
                    # for a gene and then know where to find it in the file
                    print(
                        f"INSERT INTO gex (id, ensembl_id, gene_symbol, file, offset, size) VALUES ('{gex_id}', '{ensembl_id}', '{gene_symbol}', '{f}', {seek}, {size});",
                        file=sqlf,
                    )

    print("COMMIT;", file=sqlf)
