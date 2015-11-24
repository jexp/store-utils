SRC=$1
DST=$2
SKIP_RELS=$3
SKIP_PROPS=$4
SKIP_LABELS=$5

echo "Usage: copy-store.sh source.db target.db [RELS,TO,SKIP] [props,to,skip] [Labels,To,Skip]"
echo "Database config is read from neo4j.properties file in current directory if it exists"

# heap config
export MAVEN_OPTS="-Xmx4G -Xms4G -Xmn1G -XX:+UseG1GC"

mvn compile exec:java -e -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Ddbms.pagecache.memory=2G \
      -Dexec.args="$SRC $DST $SKIP_RELS $SKIP_PROPS $SKIP_LABELS"

#-Dneo4j.version=2.3.0
