#!/bin/bash

NEO4J_HOME=${NEO4J_HOME-/usr/share/neo4j}
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
echo "To use your existing Neo4j 3.5.x installation set NEO4J_HOME to your Neo4j directory. Currently set to: $NEO4J_HOME"
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

echo
echo "Using: Heap $HEAP Pagecache $CACHE Edition '$EDITION' from '$SRC' to '$DST' skipping labels: '$SKIP_LABELS', removing nodes with labels: '$DELETE_NODES' rels: '$SKIP_RELS' props '$SKIP_PROPS' Keeping Node Ids: $KEEP_NODE_IDS"
echo
echo "Please note that you will need this memory ($CACHE + $CACHE_SRC + $HEAP) as it opens 2 databases one for reading and one for writing."
echo
# heap config
export MAVEN_OPTS="-Xmx$HEAP -Xms$HEAP -XX:+UseG1GC"
MAVEN=`which mvn`
JARFILE=`echo store-util-*.jar`

if [[ -d "$NEO4J_HOME" && -f "$JARFILE" ]]; then
   java $MAVEN_OPTS -Ddbms.pagecache.memory=$CACHE -Ddbms.pagecache.memory.source=$CACHE_SRC -classpath "$NEO4J_HOME/lib/*":$JARFILE org.neo4j.tool.StoreCopy \
   $SRC $DST $SKIP_RELS $SKIP_PROPS $SKIP_LABELS $DELETE_NODES $KEEP_NODE_IDS
else
   echo "WARNING: $NEO4J_HOME/lib does not contain any jar or store-util-*.jar file is not in the current folder."
   echo "NEO4J_HOME is : '${NEO4J_HOME}'"
   echo "store-util is : '${JARFILE}'"
   echo "Falling back to maven"
   if [[ ! -f $MAVEN ]]; then 
      echo "Apache Maven not installed"
   else
      $MAVEN clean compile exec:java -P${EDITION} -e -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Ddbms.pagecache.memory=$CACHE -Ddbms.pagecache.memory.source=$CACHE_SRC \
         -Dexec.args="$SRC $DST $SKIP_RELS $SKIP_PROPS $SKIP_LABELS $DELETE_NODES $KEEP_NODE_IDS"
   fi
fi

#-Dneo4j.version=2.3.0
