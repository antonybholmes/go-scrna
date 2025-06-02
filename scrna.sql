PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE datasets (
	id INTEGER PRIMARY KEY ASC,
	public_id TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL,
	institution TEXT NOT NULL,
	species TEXT NOT NULL,
	assembly TEXT NOT NULL,
	cells INTEGER NOT NULL,
	url TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	tags TEXT NOT NULL DEFAULT '');

CREATE INDEX datasets_public_id_idx ON datasets (public_id);
CREATE INDEX datasets_name_idx ON datasets (name);
CREATE INDEX datasets_institution_idx ON datasets (institution);
CREATE INDEX datasets_species_idx ON datasets (species);
CREATE INDEX datasets_assembly_idx ON datasets (assembly);


 