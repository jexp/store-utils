## Tools to copy Neo4j Stores

Uses the BatchInserterImpl to read a store and write the target store keeping the node-ids.
Copies the manual (legacy) index-files as is, please note it performs no index upgrade!

Ignores broken nodes and relationships and records them in `target/store-copy.log`

Also useful to skip no longer wanted properties or relationships with a certain type.
Good for store compaction and reorganization of relationships and properties as
it rewrites the store file reclaiming space that is sitting empty.

NOTE: With Neo4j 3.0 there are two different store formats, so you have to provide "enterprise" or "community" as first argument of the call!

### Store Copy

    copy-store.sh [enterprise|community] source.db target.db [RELS,TO,SKIP] [props,to,skip] [Labels,To,Skip]

Database config is read from `neo4j.properties` file in current directory if it exists.
The provided one contains:

    dbms.pagecache.memory=2G

Heap config is in the shell-script, default is:

    export MAVEN_OPTS="-Xmx4G -Xms4G -Xmn1G -XX:+UseG1GC"

**Please adapt the settings as needed for your store.**

Change the Neo4j version in pom.xml before running as needed. (Currently 3.0.0-M05)

Optionally changeable from the outside with `-Dneo4j.version=3.0.0` on the `mvn` invocation.

### Internally

Note: It calls under the hood:

    mvn compile exec:java -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Penterprise \
      -Dexec.args="source-dir target-dir [REL,TYPES,TO,IGNORE] [properties,to,ignore] [Labels,To,Ignore]"

