technology="RNA-seq"
species="Human"

institution="RDF"
 
#python make_gex_json.py \
python make_gex_bin.py \
--name="Frontiers" \
--institution="${institution}" \
--file="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/scrna/data/human/rdf/katia/5p/analysis/RK01_02_03_04_05_06_07/analysis_vdj_cgene/no_ighd/no_cc/tpm_seurat.txt.gz" \
--dir="/home/antony/development/data/modules/scrna/Human/GRCh38/RDF_Lab/Frontiers/gex/" \
--cells="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/scrna/data/human/rdf/katia/5p/analysis/RK01_02_03_04_05_06_07/analysis_vdj_cgene/no_ighd/no_cc/clusters.txt" \
--clusters="/ifs/archive/cancer/Lab_RDF/scratch_Lab_RDF/ngs/scrna/data/human/rdf/katia/5p/analysis/RK01_02_03_04_05_06_07/analysis_vdj_cgene/no_ighd/no_cc/cluster_colors_rk01-07_vdj_cgene_no_ighd_no_cc_v3.tsv"


 # #tpm_log2_seurat.txt.gz" \