# -*- coding: utf-8 -*-
"""
Encode read counts per base in 2 bytes

@author: Antony Holmes
"""
import argparse
import os
import sqlite3

import uuid_utils as uuid

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dir", help="sample name")
args = parser.parse_args()

dir = args.dir  # sys.argv[1]

data = []


db = os.path.join(dir, "scrna.db")


if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f""" CREATE TABLE datasets (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL,
	institution TEXT NOT NULL,
	species TEXT NOT NULL,
	assembly TEXT NOT NULL,
	cells INTEGER NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '');
"""
)

cursor.execute(
    f""" CREATE TABLE permissions (
	id INTEGER PRIMARY KEY ASC,
    uuid TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f""" CREATE TABLE dataset_permissions (
	dataset_id INTEGER,
    permission_id INTEGER,
    PRIMARY KEY(dataset_id, permission_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
    FOREIGN KEY (permission_id) REFERENCES permissions(id));
"""
)

rdfViewId = str(uuid.uuid7())

cursor.execute(
    f"INSERT INTO permissions (id, uuid, name) VALUES (1, '{rdfViewId}', 'rdf:view');"
)


cursor.execute(
    f""" CREATE TABLE samples (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	dataset_id INTEGER NOT NULL,
	name TEXT NOT NULL UNIQUE,
	FOREIGN KEY(dataset_id) REFERENCES datasets(id)
);
"""
)


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
    dataset_id INTEGER NOT NULL,
    label INTEGER NOT NULL UNIQUE,
	name TEXT NOT NULL UNIQUE,
	cell_count INTEGER NOT NULL,
	color TEXT NOT NULL DEFAULT '',
    FOREIGN KEY(dataset_id) REFERENCES datasets(id)
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
    f""" CREATE TABLE genes (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
	gene_id INTEGER NOT NULL UNIQUE,
    gene_symbol TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    UNIQUE(gene_id, gene_symbol) 
);
"""
)


cursor.execute(
    f""" CREATE TABLE gex (
	id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
    dataset_id INTEGER NOT NULL,
	gene_id INTEGER NOT NULL,
	url TEXT NOT NULL,
	offset INTEGER NOT NULL,
	size INTEGER NOT NULL,
    FOREIGN KEY (dataset_id) REFERENCES datasets(id)
    FOREIGN KEY (gene_id) REFERENCES genes(id)
);
"""
)


cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

dataset_id = 1

gene_map = {}
metadata_map = {}

for root, dirs, files in os.walk(dir):
    for filename in files:
        if filename == "dataset.db":
            relative_dir = root.replace(dir, "")[1:]

            # species, platform, dataset = relative_dir.split("/")

            # filepath = os.path.join(root, filename)
            # print(root, filename, relative_dir, platform, species, dataset,)

            path = os.path.join(root, filename)

            gex_path = os.path.join(relative_dir, "gex")

            conn2 = sqlite3.connect(os.path.join(root, filename))
            conn2.row_factory = sqlite3.Row

            print(filename)

            # Create a cursor object
            cursor2 = conn2.cursor()

            cursor2.execute("SELECT count(id) FROM cells")

            results = cursor2.fetchone()

            cells = results[0]

            # Execute a query to fetch data
            cursor2.execute(
                "SELECT uuid, name, institution, species, assembly, description FROM dataset"
            )

            # Fetch all results
            row = cursor2.fetchone()

            row = {
                "id": dataset_id,
                "uuid": row["uuid"],
                "name": row["name"],
                "institution": row["institution"],
                "species": row["species"],
                "assembly": row["assembly"],
                "description": row["description"],
                "cells": cells,
            }

            cursor.executemany(
                f"""INSERT INTO datasets (id, uuid, name, institution, species, assembly, description, cells) VALUES 
                (:id, :uuid, :name, :institution, :species, :assembly, :description, :cells);""",
                [row],
            )

            cursor.executemany(
                f"""INSERT INTO dataset_permissions (dataset_id, permission_id) VALUES 
                (:dataset_id, :permission_id);""",
                [{"dataset_id": dataset_id, "permission_id": 1}],
            )

            #
            # Insert samples
            #

            cursor2.execute("SELECT uuid, name FROM samples")

            row = cursor2.fetchall()

            data = []

            for sample in row:
                data.append(
                    {
                        "uuid": sample["uuid"],
                        "dataset_id": dataset_id,
                        "name": sample["name"],
                    }
                )

            cursor.executemany(
                f"""INSERT INTO samples (uuid, dataset_id, name) VALUES 
                (:uuid, :dataset_id, :name);""",
                data,
            )

            #
            # Insert clusters
            #

            cursor2.execute("SELECT uuid, label, name, cell_count, color FROM clusters")

            row = cursor2.fetchall()

            data = []
            for cluster in row:
                data.append(
                    {
                        "uuid": cluster["uuid"],
                        "dataset_id": dataset_id,
                        "label": cluster["label"],
                        "name": cluster["name"],
                        "cell_count": cluster["cell_count"],
                        "color": cluster["color"],
                    }
                )

            cursor.executemany(
                f"""INSERT INTO clusters (uuid, dataset_id, label, name, cell_count, color) VALUES 
                (:uuid, :dataset_id, :label, :name, :cell_count, :color);""",
                data,
            )

            #
            # Insert cluster metadata
            #

            cursor2.execute(
                "SELECT uuid, name FROM metadata",
            )

            row = cursor2.fetchall()

            metadata_id_map = {}
            for metadata in row:
                if metadata["uuid"] not in metadata_map:

                    metadata_map[metadata["uuid"]] = {
                        "id": len(metadata_map) + 1,
                        "uuid": metadata["uuid"],
                        "name": metadata["name"],
                    }

                    cursor.execute(
                        f"""INSERT INTO metadata (id, uuid, name) VALUES 
                        ({metadata_map[metadata["uuid"]]["id"]}, '{metadata["uuid"]}', '{metadata["name"]}');"""
                    )

            cursor2.execute(
                f"""SELECT cm.cluster_id, m.uuid, cm.value 
                FROM cluster_metadata cm
                JOIN metadata m ON m.id = cm.metadata_id
                """
            )

            row = cursor2.fetchall()

            data = []
            for cluster_metadata in row:
                cluster_id = cluster_metadata["cluster_id"]
                metadata_id = metadata_map[cluster_metadata["uuid"]]["id"]
                value = cluster_metadata["value"]

                data.append(
                    {
                        "cluster_id": cluster_id,
                        "metadata_id": metadata_id,
                        "value": value,  # Value is not stored in old db
                    }
                )

            cursor.executemany(
                f"""INSERT INTO cluster_metadata (cluster_id, metadata_id, value) VALUES 
                (:cluster_id, :metadata_id, :value);""",
                data,
            )

            #
            # Insert gex
            #

            cursor2.execute(
                "SELECT uuid, ensembl_id, gene_symbol, file, offset, size FROM gex"
            )
            row = cursor2.fetchall()
            data = []
            for gex in row:
                gene_id = gex["ensembl_id"]

                if gene_id not in gene_map:
                    gene_uuid = str(uuid.uuid7())
                    gene_map[gene_id] = {
                        "id": len(gene_map) + 1,
                        "uuid": gene_uuid,
                        "gene_id": gene_id,
                        "gene_symbol": gex["gene_symbol"],
                    }

                    cursor.execute(
                        f"""INSERT INTO genes (id, uuid, gene_id, gene_symbol) VALUES 
                        ({gene_map[gene_id]["id"]}, '{gene_uuid}', '{gene_id}', '{gex["gene_symbol"]}');"""
                    )

                data.append(
                    {
                        "uuid": gex["uuid"],
                        "dataset_id": dataset_id,
                        "gene_id": gene_map[gene_id]["id"],
                        "url": os.path.join(gex_path, gex["file"]),
                        "offset": gex["offset"],
                        "size": gex["size"],
                    }
                )

            cursor.executemany(
                f"""INSERT INTO gex (uuid, dataset_id, gene_id, url, offset, size) VALUES 
                (:uuid, :dataset_id, :gene_id, :url, :offset, :size);""",
                data,
            )

            #
            # Insert cells
            #
            cursor2.execute(
                "SELECT uuid, sample_id, cluster_id, barcode, umap_x, umap_y FROM cells"
            )
            row = cursor2.fetchall()
            data = []
            for cell in row:
                data.append(
                    {
                        "uuid": cell["uuid"],
                        "sample_id": cell["sample_id"],
                        "cluster_id": cell["cluster_id"],
                        "barcode": cell["barcode"],
                        "umap_x": cell["umap_x"],
                        "umap_y": cell["umap_y"],
                    }
                )
            cursor.executemany(
                f"""INSERT INTO cells (uuid, sample_id, cluster_id, barcode, umap_x, umap_y) VALUES 
                (:uuid, :sample_id, :cluster_id, :barcode, :umap_x, :umap_y);""",
                data,
            )

            dataset_id += 1

            conn2.close()


permissions = []


cursor.execute("COMMIT;")


cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f""" CREATE INDEX datasets_name_idx ON datasets (name);
"""
)

cursor.execute(
    f""" CREATE INDEX datasets_institution_idx ON datasets (institution);
"""
)

cursor.execute(
    f""" CREATE INDEX datasets_species_idx ON datasets (species, assembly);
"""
)

cursor.execute(
    f""" CREATE INDEX dataset_permissions_dataset_permission_idx ON dataset_permissions (dataset_id, permission_id);
"""
)

cursor.execute(
    f""" CREATE INDEX permissions_name_idx ON permissions (name);
"""
)

cursor.execute("COMMIT;")
