import collections
import gzip
import json
import os
import re
import sqlite3
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


# use cells to count cells in each cluster
counts = []

for c in df_clusters.index:
    count = len(df_cells[df_cells["Cluster"] == c])
    counts.append(count)


cluster_id_map = {c: uuid.uuid7() for i, c in enumerate(df_clusters.index)}

# df_clusters["Cells"] = counts

metadata_types = list(sorted(df_clusters.columns[1:].values))

metadata_type_map = {name: uuid.uuid7() for name in metadata_types}

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
	id TEXT PRIMARY KEY ASC,
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
	id TEXT PRIMARY KEY ASC,
	dataset_id TEXT NOT NULL,
	name TEXT NOT NULL UNIQUE,
	FOREIGN KEY(dataset_id) REFERENCES dataset(id)
);
"""
)

cursor.execute(
    f""" CREATE TABLE metadata_types (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	UNIQUE(name));
"""
)

cursor.execute(
    f""" CREATE TABLE metadata (
	id TEXT PRIMARY KEY ASC,
	metadata_type_id TEXT NOT NULL,
	value TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	color TEXT NOT NULL DEFAULT '',
	UNIQUE(metadata_type_id, value, color),
	FOREIGN KEY(metadata_type_id) REFERENCES metadata_types(id));
"""
)

cursor.execute(
    f""" CREATE TABLE clusters (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL UNIQUE,
	cell_count INTEGER NOT NULL,
	color TEXT NOT NULL DEFAULT ''
);
"""
)

cursor.execute(
    f""" CREATE TABLE cluster_metadata (
	id TEXT PRIMARY KEY ASC,
	cluster_id TEXT NOT NULL,
	metadata_id TEXT NOT NULL,
	UNIQUE(cluster_id, metadata_id),
	FOREIGN KEY(cluster_id) REFERENCES clusters(id),
	FOREIGN KEY(metadata_id) REFERENCES metadata(id)  
);
"""
)

cursor.execute(
    f""" CREATE TABLE cells (
	id TEXT PRIMARY KEY ASC,
	cluster_id TEXT NOT NULL, 
	sample_id TEXT NOT NULL,
	barcode	TEXT NOT NULL, 
	umap_x REAL NOT NULL, 
	umap_y REAL NOT NULL, 
	FOREIGN KEY (cluster_id) REFERENCES clusters(id),
	FOREIGN KEY (sample_id) REFERENCES samples(id)  
);
"""
)


cursor.execute(
    f""" CREATE TABLE gex (
	id TEXT PRIMARY KEY ASC,
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
    f"INSERT INTO dataset (id, name, institution, species, assembly, cells, dir) VALUES ('{dataset_id}', '{name}', '{institution}', '{species}', '{assembly}', {df_cells.shape[0]}, '{dir}');",
)


sample_map = {}
for i, sample in enumerate(sorted(df_cells["Sample"].unique())):
    sample_id = uuid.uuid7()  # generate("0123456789abcdefghijklmnopqrstuvwxyz", 12)
    cursor.execute(
        f"INSERT INTO samples (id, dataset_id, name) VALUES ('{sample_id}', '{dataset_id}', '{sample}');",
    )
    sample_map[sample] = sample_id  # i + 1

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

for idx, (cluster, row) in enumerate(df_clusters.iterrows()):
    cluster_id = cluster_id_map[row.name]

    cursor.execute(
        f"INSERT INTO clusters (id, name, cell_count, color) VALUES ('{cluster_id}', '{cluster}',  {counts[idx]}, '{row["Color"]}');",
    )

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

for name in metadata_types:
    metadata_id = metadata_type_map[name]  # uuid.uuid7()
    cursor.execute(
        f"INSERT INTO metadata_types (id, name) VALUES ('{metadata_id}', '{name}');",
    )

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

metadata_map = collections.defaultdict(lambda: {})

for i, name in enumerate(metadata_types):
    metadata_type_id = metadata_type_map[name]
    for v in sorted(df_clusters[name].unique()):
        metadata_id = uuid.uuid7()
        cursor.execute(
            f"INSERT INTO metadata (id, metadata_type_id, value) VALUES ('{metadata_id}', '{metadata_type_id}',  '{v}');",
        )
        metadata_map[name][v] = metadata_id  # index

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")


# clusters can have metadata attached to them
for idx, (i, row) in enumerate(df_clusters.iterrows()):
    cluster_id = cluster_id_map[row.name]
    for j, metadata_type in enumerate(metadata_types):
        cluster_metadata_id = uuid.uuid7()
        metadata_value = row[j + 1]

        metadata_id = metadata_map[metadata_type][metadata_value]
        cursor.execute(
            f"INSERT INTO cluster_metadata (id, cluster_id, metadata_id) VALUES ('{cluster_metadata_id}', '{cluster_id}', '{metadata_id}');",
        )

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

for i, row in df_cells.iterrows():
    cell_id = uuid.uuid7()
    cursor.execute(
        f"INSERT INTO cells (id, cluster_id, sample_id, barcode, umap_x, umap_y) VALUES ('{cell_id}', '{cluster_id_map[row["Cluster"]]}', '{sample_map[row["Sample"]]}', '{row["Barcode"]}', {row["UMAP-1"]}, {row["UMAP-2"]});",
    ),

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
                    f"INSERT INTO gex (id, ensembl_id, gene_symbol, file, offset, size) VALUES ('{gex_id}', '{ensembl_id}', '{gene_symbol}', '{f}', {dat_offset}, {size});",
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
