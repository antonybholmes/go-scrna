CREATE INDEX samples_dataset_name_idx ON samples (name);
CREATE INDEX expression_gene_id_sample_id_idx ON expression (gene_id);

CREATE INDEX genes_hugo_id_idx ON genes (hugo_id);
CREATE INDEX genes_ensembl_id_idx ON genes (ensembl_id);
CREATE INDEX genes_refseq_id_idx ON genes (refseq_id);
CREATE INDEX genes_gene_symbol_idx ON genes (gene_symbol);
