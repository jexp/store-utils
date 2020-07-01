## Tool to copy Neo4j Stores

From Neo4j 4.0 please use the [official tool](https://neo4j.com/docs/operations-manual/4.0/tools/copy/) that has the same capabilities.

Uses the BatchInserterImpl to read a store and write the target store keeping the node-ids.
Copies the manual (legacy) index-files as is, please note it performs no index upgrade!

You will have to recreate any schema indexes too.

Ignores broken nodes and relationships and records them in `target/store-copy.log`

Also useful to skip no longer wanted properties, relationships with a certain type.
Or of certain labels and even nodes with certain labels.

Good for store compaction and reorganization of relationships and properties as
it rewrites the store file reclaiming space that is sitting empty.

NOTE: With Neo4j 3.x there are two different store formats, so you have to provide "enterprise" or "community" as first argument of the call!

You can now also decide if you want to compact the node-store, then you have to pass "false" as the parameter for keep-node-ids.

### Usage

Grab the release for your Neo4j version from: https://github.com/jexp/store-utils/releases

Define the $NEO4J_HOME, the $NEO4J_HOME/lib should contain all the jars

```
unzip store-util-*-release.zip 
cd store-util-*/

#feel free to change this
export NEO4J_HOME=/usr/share/neo4j

#IMPORTANT: maven and internet access will be required if NEO4J_HOME is not set properly
[ ! $(ls $NEO4J_HOME/lib/*) ] && echo "NEO4J_HOME does not contain the libraries, will fall back to maven"


# remove target db
rm -rf /path/to/fixed.db

./copy-store.sh community /path/to/source.db /path/to/fixed.db
```

#### NOTICE

**With Neo4j 3.5.x the location of the `store_lock` file changed to one level above the database directory, so please make sure that your source and target db don't share the same parent directory**

Then you would see an error like this:

```
Exception in thread "main" org.neo4j.kernel.StoreLockException: Unable to obtain lock on store lock file: 
/path/to/parent-folder/store_lock. 
Please ensure no other process is using this database, and that the directory is writable (required even for read-only access)
```

### Config 

Config will read from `neo4j.properties` file in current directory if it exists, but command line options override.

neo4j.properties

```
source_db_dir=
target_db_dir=

keep_node_ids=true

properties_to_ignore=
labels_to_ignore=
labels_to_delete=
rel_types_to_ignore=

store_copy_log_dir=
bad_entries_log_dir=
```

### General Usage

    copy-store.sh [enterprise|community] source.db target.db [RELS,TO,SKIP] [props,to,skip] [Labels,To,Skip] [Labels,To,Delete,Nodes] [keep-node-ids:true/false]


The provided script contains these settings for page-cache (note you can configure a different, smaller setting for the source store than the target store).

    dbms.pagecache.memory.source=2G
    dbms.pagecache.memory=2G

Heap config is in the shell-script, default is: 4 GB Heap

    export MAVEN_OPTS="-Xmx4G -Xms4G -Xmn1G -XX:+UseG1GC"

**Please adapt the settings as needed for your store.**

**Please note that you will need the memory for (source-page-cache + target-page-cache + 1x heap) as it opens 2 databases one for reading and one for writing.**

Change the Neo4j version in pom.xml before running as needed. (Currently 3.4.5)

Optionally changeable from the outside with `-Dneo4j.version=3.4.5` on the `mvn` invocation.

### Internally



Note: maven is called under the hood :

    mvn compile exec:java -Dexec.mainClass="org.neo4j.tool.StoreCopy" -Penterprise \
      -Dexec.args="source-dir target-dir [REL,TYPES,TO,IGNORE] [properties,to,ignore] [Labels,To,Ignore] [Labels,To,Delete,Nodes] [keep-node-ids:true/false]"

