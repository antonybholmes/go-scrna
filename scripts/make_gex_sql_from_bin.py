import argparse
import collections
import gzip
import json
import os
import re
import sqlite3
import struct
import sys

import msgpack
import numpy as np
import pandas as pd
import uuid_utils as uuid
from nanoid import generate
from pysam import index

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


# use cells to count cells in each cluster
counts = []

for c in df_clusters.index:
    count = len(df_cells[df_cells["Cluster"] == c])
    counts.append(count)


# map cluster id to uuid e.g. 1 -> 'c4f8e2a0-1d5b-11ee-be56-0242ac120002'
cluster_id_map = {
    c: {"uuid": uuid.uuid7(), "index": i + 1} for i, c in enumerate(df_clusters.index)
}

# df_clusters["Cells"] = counts

metadata_types = list(sorted(df_clusters.columns[1:].values))

metadata_type_map = {
    name: {"uuid": uuid.uuid7(), "index": i + 1}
    for i, name in enumerate(metadata_types)
}

db = os.path.join(dir, "dataset.db")


if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f""" CREATE TABLE dataset (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL, 
	institution TEXT NOT NULL, 
	species TEXT NOT NULL, 
	assembly TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	cells INTEGER NOT NULL,
	tags TEXT NOT NULL DEFAULT '',
	dir TEXT NOT NULL
);
"""
)

cursor.execute(
    f""" CREATE TABLE samples (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	dataset_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
	FOREIGN KEY(dataset_id) REFERENCES dataset(id)
);
"""
)

# cursor.execute(
#     f""" CREATE TABLE metadata_types (
# 	id INTEGER PRIMARY KEY,
#     uuid TEXT NOT NULL UNIQUE,
# 	name TEXT NOT NULL,
# 	description TEXT NOT NULL DEFAULT '',
# 	UNIQUE(name));
# """
# )

cursor.execute(
    f""" CREATE TABLE metadata (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	description TEXT NOT NULL DEFAULT '');
"""
)

cursor.execute(
    f""" CREATE TABLE clusters (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
    label INTEGER NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	cell_count INTEGER NOT NULL,
	color TEXT NOT NULL DEFAULT ''
);
"""
)

cursor.execute(
    f""" CREATE TABLE cluster_metadata (
	cluster_id INTEGER NOT NULL,
	metadata_id INTEGER NOT NULL,
    value TEXT NOT NULL,
	PRIMARY KEY(cluster_id, metadata_id),
	FOREIGN KEY(cluster_id) REFERENCES clusters(id),
	FOREIGN KEY(metadata_id) REFERENCES metadata(id)  
);
"""
)

cursor.execute(
    f""" CREATE TABLE cells (
    id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	sample_id INTEGER NOT NULL,
    cluster_id INTEGER NOT NULL, 
	barcode	TEXT NOT NULL, 
	umap_x REAL NOT NULL, 
	umap_y REAL NOT NULL,
    UNIQUE(sample_id, cluster_id, barcode),
	FOREIGN KEY (cluster_id) REFERENCES clusters(id),
	FOREIGN KEY (sample_id) REFERENCES samples(id)  
);
"""
)


cursor.execute(
    f""" CREATE TABLE gex (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	ensembl_id TEXT NOT NULL,
	gene_symbol TEXT NOT NULL, 
	file TEXT NOT NULL,
	offset INTEGER NOT NULL,
	size INTEGER NOT NULL
);
"""
)


cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

dataset_id = uuid.uuid7()  # = generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)


cursor.execute(
    f"INSERT INTO dataset (id, uuid, name, institution, species, assembly, cells, dir) VALUES (1, '{dataset_id}', '{name}', '{institution}', '{species}', '{assembly}', {df_cells.shape[0]}, '{dir}');",
)


sample_map = {}
for i, sample in enumerate(sorted(df_cells["Sample"].unique())):
    sample_id = uuid.uuid7()  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
    cursor.execute(
        f"INSERT INTO samples (id, uuid, dataset_id, name) VALUES ({i + 1}, '{sample_id}', 1, '{sample}');",
    )
    sample_map[sample] = {"uuid": sample_id, "index": i + 1}

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

for idx, (cluster, row) in enumerate(df_clusters.iterrows()):
    cluster_id = cluster_id_map[row.name]["uuid"]

    # row name is the cluster label, a number
    label = int(row.name)
    cursor.execute(
        f"INSERT INTO clusters (id, uuid, label, name, cell_count, color) VALUES ({idx + 1}, '{cluster_id}', {label}, '{cluster}',  {counts[idx]}, '{row["Color"]}');",
    )

cursor.execute("COMMIT;")

# cursor.execute("BEGIN TRANSACTION;")

# for i, name in enumerate(metadata_types):
#     metadata_id = metadata_type_map[name]  # uuid.uuid7()
#     cursor.execute(
#         f"INSERT INTO metadata_types (id, uuid, name) VALUES ({i + 1}, '{metadata_id}', '{name}');",
#     )

# cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

metadata_map = collections.defaultdict(lambda: {})


for i, name in enumerate(metadata_types):
    metadata_type_id = metadata_type_map[name]["uuid"]
    idx = metadata_type_map[name]["index"]
    cursor.execute(
        f"INSERT INTO metadata (id, uuid, name) VALUES ({idx}, '{metadata_type_id}',  '{name}');",
    )


cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")


# clusters can have metadata attached to them
for idx, (i, row) in enumerate(df_clusters.iterrows()):
    cluster_index = cluster_id_map[row.name]["index"]
    cluster_id = cluster_id_map[row.name]["uuid"]

    for j, metadata_type in enumerate(metadata_types):
        metadata_index = metadata_type_map[metadata_type]["index"]
        metadata_value = row[j + 1]

        cursor.execute(
            f"INSERT INTO cluster_metadata (cluster_id, metadata_id, value) VALUES ({cluster_index}, {metadata_index}, '{metadata_value}');",
        )

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

for i, row in df_cells.iterrows():
    cell_id = uuid.uuid7()
    cursor.execute(
        f"INSERT INTO cells (uuid, sample_id, cluster_id, barcode, umap_x, umap_y) VALUES ('{cell_id}', {sample_map[row["Sample"]]["index"]}, {cluster_id_map[row["Cluster"]]["index"]}, '{row["Barcode"]}', {row["UMAP-1"]}, {row["UMAP-2"]});",
    )

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

for f in sorted(os.listdir(gex_dir)):
    # if f.endswith(".json.gz"):
    #     # cursor.execute(f)
    #     with gzip.open(os.path.join(gex_dir, f), "r") as fin:
    #         data = json.load(fin)

    #         for d in data:
    #             id = d["id"]
    #             sym = d["sym"]

    #             cursor.execute(
    #                 f"INSERT INTO gex (ensembl_id, gene_symbol, file, offset) VALUES ('{id}', '{sym}', '{f}');",
    #                 ,
    #             )

    if f.endswith(".bin"):
        # cursor.execute(f)
        file = os.path.join(gex_dir, f)
        with open(file, "rb") as fin:

            magic = struct.unpack("<I", fin.read(4))[0]
            print("Magic:", file, magic)

            # Step 1: Read the offset table entry

            version = struct.unpack("<I", fin.read(4))[0]
            print("Version:", version)

            cells = struct.unpack("<I", fin.read(4))[0]
            print("Cells:", cells)

            # num genes
            # num_entries = struct.unpack("<I", fin.read(4))[0]

            # each entry is 8 bytes (4 bytes offset, 4 bytes size)
            # data = fin.read(num_entries * 4 * 2)

            # Unpack as  unsigned ints (little-endian)
            # offsets = struct.unpack(f"<{num_entries*2}I", data)

            print(f, cells)

            # magic + version + num entries
            dat_offset = 4 + 4 + 4

            for i in range(0, cells):
                # size of object
                size = struct.unpack("<I", fin.read(4))[0]

                gene_id_len = struct.unpack("<H", fin.read(2))[0]
                ensembl_id = fin.read(gene_id_len).decode("utf-8")

                gene_symbol_len = struct.unpack("<H", fin.read(2))[0]
                gene_symbol = fin.read(gene_symbol_len).decode("utf-8")

                print("Size:", size, ensembl_id, gene_symbol)

                # now we can get the id and gene symbol
                gex_id = uuid.uuid7()

                # log the offset and size in the db so we can search
                # for a gene and then know where to find it in the file
                cursor.execute(
                    f"INSERT INTO gex (uuid, ensembl_id, gene_symbol, file, offset, size) VALUES ('{gex_id}', '{ensembl_id}', '{gene_symbol}', '{f}', {dat_offset}, {size});",
                )

                # size does not include the 4 bytes of size itself
                # so we must add it to get to the next record
                dat_offset += 4 + size

                # skip in file to next record
                fin.seek(dat_offset, 0)

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f""" CREATE INDEX clusters_name_idx ON clusters (name);
"""
)

cursor.execute(
    f""" CREATE INDEX cells_barcode_idx ON cells (barcode);
"""
)

cursor.execute(
    f""" CREATE INDEX cells_cluster_id_idx ON cells (cluster_id);
"""
)

cursor.execute(
    f""" CREATE INDEX cells_sample_id_idx ON cells (sample_id);
"""
)

cursor.execute(
    f"""CREATE INDEX gex_ensembl_id_idx ON gex (ensembl_id);
"""
)

cursor.execute(
    f"""CREATE INDEX gex_gene_symbol_idx ON gex (gene_symbol);
"""
)

cursor.execute("COMMIT;")
