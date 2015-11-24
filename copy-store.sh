SRC=$1
DST=$2
RELS=$3
PROPS=$4
LABELS=$5

mvn compile exec:java -e -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Ddbms.pagecache.memory=2G \
      -Dexec.args="$SRC $DST $RELS $PROPS $LABELS"
