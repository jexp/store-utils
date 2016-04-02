EDITION=${1-community}
shift
SRC=$1
DST=$2
SKIP_RELS=$3
SKIP_PROPS=$4
SKIP_LABELS=$5
HEAP=4G
CACHE=2G
echo "Usage: copy-store.sh [community|enterprise] source.db target.db [RELS,TO,SKIP] [props,to,skip] [Labels,To,Skip]"
echo "Database config is read from neo4j.properties file in current directory if it exists"
echo "Using: Heap $HEAP Pagecache $CACHE Edition $EDITION from $SRC to $DST skipping labels: $SKIP_LABELS rels: $SKIP_RELS props $SKIP_PROPS"
# heap config
export MAVEN_OPTS="-Xmx$HEAP -Xms$HEAP -Xmn1G -XX:+UseG1GC"

mvn clean compile exec:java -P${EDITION} -e -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Ddbms.pagecache.memory=$CACHE \
      -Dexec.args="$SRC $DST $SKIP_RELS $SKIP_PROPS $SKIP_LABELS"

#-Dneo4j.version=2.3.0
