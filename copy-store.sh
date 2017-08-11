#!/bin/bash

EDITION=${1-community}
shift
SRC=${1%%:*}
SVER=${1##*:}
DST=${2%%:*}
DVER=${2##*:}
SKIP_RELS=$3
SKIP_PROPS=$4
SKIP_LABELS=$5
DELETE_NODES=$6
KEEP_NODE_IDS=$7
HEAP=4G
CACHE=2G
CACHE_SRC=1G
#$CACHE
echo "Usage: copy-store.sh [community|enterprise] source.db:version target.db:version [RELS,TO,SKIP] [props,to,skip] [Labels,To,Skip] [Labels,To,Delete,Nodes]"

if [[ "$EDITION" != "enterprise" && "$EDITION" != "community" ]]
then
    echo "ATTENTION: The parameter '$EDITION' you passed in for the edition is neither 'community' nor 'enterprise'. Aborting."
    exit
fi
if [[ "$SRC" = "" || "$DST" = "" ]]
then
    echo "ATTENTION: Source '$SRC' or target '$DST' not provided. Aborting."
    exit
fi

if [[ ! -d $SRC ]]
then
    echo "ATTENTION: Source '$SRC' is not a directory. Aborting."
    exit
fi

if [[ -d $DST ]]
then
    echo "ATTENTION: Target '$DST' already exists. Aborting."
    exit
fi

echo "Using: Heap $HEAP Pagecache $CACHE Edition '$EDITION' from '$SRC' (version $SVER) to '$DST' (version $DVER) skipping labels: '$SKIP_LABELS', removing nodes with labels: '$DELETE_NODES' rels: '$SKIP_RELS' props '$SKIP_PROPS' Keeping Node Ids: $KEEP_NODE_IDS"
echo
echo "Please note that you will need this memory ($CACHE + $CACHE_SRC + $HEAP) as it opens 2 databases one for reading and one for writing."
# heap config

echo "Building binary packages (jars) for all versions"
mvn install -P${EDITION} 2>&1 | grep -v '\[\(INFO\|WARNING\)\]'

pushd `pwd`

cd runner

export MAVEN_OPTS="-Xmx$HEAP -Xms$HEAP -XX:+UseG1GC"
mvn exec:java -P${EDITION} -e -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Ddbms.pagecache.memory=$CACHE -Ddbms.pagecache.memory.source=$CACHE_SRC \
      -Dexec.args="$SRC:$SVER $DST:$DVER $SKIP_RELS $SKIP_PROPS $SKIP_LABELS $DELETE_NODES $KEEP_NODE_IDS"
popd
