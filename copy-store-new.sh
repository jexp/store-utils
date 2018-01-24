#!/bin/bash

EDITION=${1-community}
shift
SRC=$1
DST=$2
SKIP_RELS=$3
SKIP_PROPS=$4
SKIP_LABELS=$5
DELETE_NODES=$6
KEEP_NODE_IDS=$7
HEAP=4G
CACHE=2G
CACHE_SRC=1G
#$CACHE
echo "Usage: copy-store.sh [community|enterprise] source.db target.db [RELS,TO,SKIP] [props,to,skip] [Labels,To,Skip] [Labels,To,Delete,Nodes]"

if [[ "$EDITION" != "enterprise" && "$EDITION" != "community" ]]
then
    echo "ATTENTION: The parameter '$EDITION' you passed in for the edition is neither 'community' nor 'enterprise'. Aborting."
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

echo "Using: Heap $HEAP Pagecache $CACHE Edition '$EDITION' from '$SRC' to '$DST' skipping labels: '$SKIP_LABELS', removing nodes with labels: '$DELETE_NODES' rels: '$SKIP_RELS' props '$SKIP_PROPS' Keeping Node Ids: $KEEP_NODE_IDS"
echo
echo "Please note that you will need this memory ($CACHE + $CACHE_SRC + $HEAP) as it opens 2 databases one for reading and one for writing."
# heap config
export MAVEN_OPTS="-Xmx$HEAP -Xms$HEAP -XX:+UseG1GC"

mvn clean compile exec:java -P${EDITION} -e -Dexec.mainClass="org.neo4j.tool.StoreCopyNew" -Ddbms.pagecache.memory=$CACHE -Ddbms.pagecache.memory.source=$CACHE_SRC \
      -Dexec.args="$SRC $DST $SKIP_RELS $SKIP_PROPS $SKIP_LABELS $DELETE_NODES $KEEP_NODE_IDS"

#-Dneo4j.version=2.3.0
