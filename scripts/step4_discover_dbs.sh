dir=../data/modules/scrna

python discover_dbs.py --dir=${dir} 

 
rm ${dir}/scrna.db
cat scrna.sql | sqlite3 ${dir}/scrna.db
cat ${dir}/scrna.sql | sqlite3 ${dir}/scrna.db
