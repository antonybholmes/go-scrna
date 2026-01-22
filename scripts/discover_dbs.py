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

for root, dirs, files in os.walk(dir):
    for filename in files:
        if filename == "dataset.db":
            relative_dir = root.replace(dir, "")[1:]

            print(relative_dir)

            # species, platform, dataset = relative_dir.split("/")

            # filepath = os.path.join(root, filename)
            # print(root, filename, relative_dir, platform, species, dataset,)

            path = os.path.join(root, filename)

            conn = sqlite3.connect(os.path.join(root, filename))

            conn.row_factory = sqlite3.Row
            print(filename)

            # Create a cursor object
            cursor = conn.cursor()

            cursor.execute("SELECT count(id) FROM cells")

            results = cursor.fetchone()

            cells = results[0]

            # Execute a query to fetch data
            cursor.execute(
                "SELECT id, name, institution, species, assembly, description FROM dataset"
            )

            # Fetch all results
            results = cursor.fetchall()

            # Print the results
            for row in results:
                row = {
                    "id": str(uuid.uuid7()),
                    "dataset_id": row["id"],
                    "name": row["name"],
                    "institution": row["institution"],
                    "species": row["species"],
                    "assembly": row["assembly"],
                    "description": row["description"],
                    "cells": cells,
                    "url": path,
                }

                # row.append(dataset)
                data.append(row)

            conn.close()


db = os.path.join(dir, "datasets.db")


if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
cursor = conn.cursor()

cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA foreign_keys = ON;")

cursor.execute("BEGIN TRANSACTION;")

cursor.execute(
    f""" CREATE TABLE datasets (
	id TEXT PRIMARY KEY ASC,
    dataset_id TEXT NOT NULL,
	name TEXT NOT NULL,
	institution TEXT NOT NULL,
	species TEXT NOT NULL,
	assembly TEXT NOT NULL,
	cells INTEGER NOT NULL,
	url TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '');
"""
)

cursor.execute(
    f""" CREATE TABLE permissions (
	id TEXT PRIMARY KEY ASC,
	name TEXT NOT NULL);
"""
)

cursor.execute(
    f""" CREATE TABLE dataset_permissions (
	id TEXT PRIMARY KEY ASC,
	dataset_id TEXT,
    permission_id TEXT,
    UNIQUE(dataset_id, permission_id),
    FOREIGN KEY (dataset_id) REFERENCES datasets(id),
    FOREIGN KEY (permission_id) REFERENCES permissions(id));
"""
)

cursor.execute("COMMIT;")

cursor.execute("BEGIN TRANSACTION;")

rdfViewId = str(uuid.uuid7())

cursor.execute(
    f"INSERT INTO permissions (id, name) VALUES ('{rdfViewId}', 'rdf:view');"
)

cursor.executemany(
    f"""INSERT INTO datasets (id, dataset_id, name, institution, species, assembly, description, cells, url) VALUES 
    (:id, :dataset_id, :name, :institution, :species, :assembly, :description, :cells, :url);""",
    data,
)

permissions = []

for row in data:
    id = str(uuid.uuid7())
    dataset_id = row["id"]

    permissions.append({"id": id, "dataset_id": dataset_id, "permission_id": rdfViewId})

cursor.executemany(
    f"""INSERT INTO dataset_permissions (id, dataset_id, permission_id) VALUES 
    (:id, :dataset_id, :permission_id);""",
    permissions,
)

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
