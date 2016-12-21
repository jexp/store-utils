SRC=$1
DST=$2
SKIP_RELS=$3
SKIP_PROPS=$4
SKIP_LABELS=$5
HEAP=4G
CACHE=2G
echo "Usage: copy-store.sh source.db target.db [RELS,TO,SKIP] [props,to,skip] [Labels,To,Skip]"

if [[ "$SRC" = "enterprise" || "$SRC" = "community" ]]
then
    echo "ATTENTION: The source '$SRC' you passed is the same as an edition 'community/enterprise' for the 3.x version of this tool. Aborting."
    exit
fi
if [[ "$SRC" = "" || "$DST" = "" ]]
then
    echo "ATTENTION: Source '$SRC' or target '$DST' directory not provided. Aborting."
    exit
fi

if [[ ! -d $SRC ]]
then
    echo "ATTENTION: Source '$SRC' is not a directory. Aborting."
    exit
fi

echo "Database config is read from neo4j.properties file in current directory if it exists"

# heap config
export MAVEN_OPTS="-Xmx$HEAP -Xms$HEAP -Xmn1G -XX:+UseG1GC"

mvn compile exec:java -e -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Ddbms.pagecache.memory=$CACHE \
      -Dexec.args="$SRC $DST $SKIP_RELS $SKIP_PROPS $SKIP_LABELS"

#-Dneo4j.version=2.3.0
