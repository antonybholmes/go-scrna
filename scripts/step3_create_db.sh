for f in `find data/modules/scrna | grep -P 'dataset.sql'`
do
    name=`echo ${f} | sed -r 's/.sql//'`
    rm ${name}.db
    cat dataset.sql | sqlite3 ${name}.db
    cat ${f} | sqlite3 ${name}.db
done