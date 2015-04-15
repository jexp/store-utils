## Tools to copy and compare Neo4j Stores

Uses the GraphDatabaseService to read a store and the batch-inserter API to write the target store keeping the node-ids.
Copies the index-files as is.
Ignores broken nodes and relationships.

Also useful to skip no longer wanted properties or relationships with a certain type. Good for store compaction as it
rewrites the store file reclaiming space that is sitting empty.

Change the Neo4j version in pom.xml before running. (Currently 1.9.9)

### Store Copy

Usage:

    mvn compile exec:java -Dexec.mainClass="org.neo4j.tool.StoreCopy" \
      -Dexec.args="source-dir target-dir [rel,types,to,ignore] [properties,to,ignore]"

# Store Compare

    mvn compile exec:java -Dexec.mainClass="org.neo4j.tool.StoreComparer" \
      -Dexec.args="source-dir target-dir [rel,types,to,ignore] [properties,to,ignore]"
