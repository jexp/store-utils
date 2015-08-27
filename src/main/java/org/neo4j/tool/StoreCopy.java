package org.neo4j.tool;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollection;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

public class StoreCopy {

    private static final Label[] NO_LABELS = new Label[0];
    private static PrintWriter logs;

    @SuppressWarnings("unchecked")
    public static Map<String, String> config() {
        return (Map) MapUtil.map(
                "neostore.nodestore.db.mapped_memory", "100M",
                "neostore.relationshipstore.db.mapped_memory", "500M",
                "neostore.relationshipgroupstore.db.mapped_memory", "10M",
                "neostore.propertystore.db.mapped_memory", "300M",
                "neostore.propertystore.db.strings.mapped_memory", "1G",
                "neostore.propertystore.db.arrays.mapped_memory", "300M",
                "neostore.propertystore.db.index.keys.mapped_memory", "100M",
                "neostore.propertystore.db.index.mapped_memory", "100M",
                "use_memory_mapped_buffers","true",
                "cache_type", "none"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StoryCopy [--scan-rel-store] source target [rel,types,to,ignore] [properties,to,ignore] [labels,to,ignore]");
            System.err.println("  --scan-rel-store indicates that relationships should be copied by scanning every possible relationship ID in");
            System.err.println("      the relationship store instead of just copying the relationships referenced by nodes.  In a consistent ");
            System.err.println("      database both methods are equivalent.  --scan-rel-store is much slower for relationship stores that need a");
            System.err.println("      lot of compaction, but it uses less memory.");
            return;
        }

        int argBase = 0;
        boolean useScanStore = false;

        if (args[0].equals("--scan-rel-store")) {
            argBase++;
            useScanStore = true;
        }

        String sourceDir = args[argBase++];
        String targetDir = args[argBase++];
        Set<String> ignoreRelTypes = splitOptionIfExists(args, argBase++);
        Set<String> ignoreProperties = splitOptionIfExists(args, argBase++);
        Set<String> ignoreLabels = splitOptionIfExists(args, argBase++);
        System.out.printf("Copying from %s to %s ingoring rel-types %s ignoring properties %s ignoring labels %s %n", sourceDir, targetDir, ignoreRelTypes, ignoreProperties,ignoreLabels);
        copyStore(sourceDir, targetDir, useScanStore, ignoreRelTypes, ignoreProperties,ignoreLabels);
    }

    private static Set<String> splitOptionIfExists(String[] args, final int index) {
        if (args.length <= index) return emptySet();
        return new HashSet<String>(asList(args[index].toLowerCase().split(",")));
    }

    private static void copyStore(String sourceDir, String targetDir, boolean useScanStore, Set<String> ignoreRelTypes, Set<String> ignoreProperties, Set<String> ignoreLabels) throws Exception {
        final File target = new File(targetDir);
        final File source = new File(sourceDir);
        if (target.exists()) {
            FileUtils.deleteRecursively(target);
            // throw new IllegalArgumentException("Target Directory already exists "+target);
        }
        if (!source.exists()) throw new IllegalArgumentException("Source Database does not exist " + source);

        Pair<Long, Long> highestIds = getHighestNodeId(source);
        BatchInserter targetDb = BatchInserters.inserter(target.getAbsolutePath(), config());
        BatchInserter sourceDb = BatchInserters.inserter(source.getAbsolutePath(), config());
        logs = new PrintWriter(new FileWriter(new File(target, "store-copy.log")));

        long firstNode = firstNode(sourceDb, highestIds.first());

        PrimitiveLongCollection relationshipIds = copyNodes(sourceDb, targetDb, useScanStore, ignoreProperties, ignoreLabels, highestIds.first());
        copyRelationships(sourceDb, targetDb, ignoreRelTypes, ignoreProperties, relationshipIds, firstNode, highestIds.other());

        targetDb.shutdown();
        sourceDb.shutdown();
        logs.close();
        copyIndex(source, target);
    }

    private static Pair<Long, Long> getHighestNodeId(File source) {
        GraphDatabaseAPI api = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(source.getAbsolutePath());
        IdGeneratorFactory idGenerators = api.getDependencyResolver().resolveDependency(IdGeneratorFactory.class);
        long highestNodeId = idGenerators.get(IdType.NODE).getHighestPossibleIdInUse();
        long highestRelId = idGenerators.get(IdType.RELATIONSHIP).getHighestPossibleIdInUse();
        api.shutdown();
        return Pair.of(highestNodeId, highestRelId);
    }

    private static void copyIndex(File source, File target) throws IOException {
        final File indexFile = new File(source, "index.db");
        if (indexFile.exists()) {
            FileUtils.copyFile(indexFile, new File(target, "index.db"));
        }
        final File indexDir = new File(source, "index");
        if (indexDir.exists()) {
            FileUtils.copyRecursively(indexDir, new File(target, "index"));
        }
    }

    private static void copyRelationships(BatchInserter sourceDb, BatchInserter targetDb, Set<String> ignoreRelTypes, Set<String> ignoreProperties, PrimitiveLongCollection relationshipIds, long firstNode, long highestRelId) {
        long time = System.currentTimeMillis();
        long notFound = 0;
        long count = 0;
        long total = relationshipIds != null ? relationshipIds.size() : highestRelId;
        Iterator<Long> iterator = getRelationshipIdIterator(relationshipIds, highestRelId);
        while (iterator.hasNext()) {
            long relId = iterator.next();
            BatchRelationship rel = null;
            try {
                if (count % 10000 == 0) {
                    System.out.print(".");
                }
                if (count % 500000 == 0) {
                    flushCache(sourceDb, firstNode);
                    System.out.println(" " + count + " / " + total + " (" + 100 *((float)count / total) + "%)");
                }
                rel = sourceDb.getRelationshipById(relId);
                if (ignoreRelTypes.contains(rel.getType().name().toLowerCase())) continue;
                createRelationship(targetDb, sourceDb, rel, ignoreProperties);
                count++;
            } catch (InvalidRecordException nfe) {
                notFound++;
            }
        }
        System.out.println("\n copying of " + count + " relationships took "+(System.currentTimeMillis()-time)+" ms. Not found "+notFound);

    }

    private static long firstNode(BatchInserter sourceDb, long highestNodeId) {
        int node = -1;
        while (++node <= highestNodeId) {
            if (sourceDb.nodeExists(node)) return node;
        }
        return -1;
    }

    private static void createRelationship(BatchInserter targetDb, BatchInserter sourceDb, BatchRelationship rel, Set<String> ignoreProperties) {
        long startNodeId = rel.getStartNode();
        long endNodeId = rel.getEndNode();
        final RelationshipType type = rel.getType();
        try {
            targetDb.createRelationship(startNodeId, endNodeId, type, getProperties(sourceDb.getRelationshipProperties(rel.getId()), ignoreProperties));
        } catch (Exception ire) {
            addLog(rel, "create Relationship: " + startNodeId + "-[:" + type + "]" + "->" + endNodeId, ire.getMessage());
        }
    }

    private static PrimitiveLongCollection copyNodes(BatchInserter sourceDb, BatchInserter targetDb, boolean dontStoreRelIds, Set<String> ignoreProperties, Set<String> ignoreLabels, long highestNodeId) {
        long time = System.currentTimeMillis();
        int node = -1;
        long notFound = 0;
        PrimitiveLongSet relationshipIds = dontStoreRelIds ? null : Primitive.offHeapLongSet();
        while (++node <= highestNodeId) {
            try {
              if (node % 10000 == 0) {
                  System.out.print(".");
              }
              if (node % 500000 == 0) {
                  flushCache(sourceDb, node);
                  logs.flush();
                  System.out.println(" " + node + " / " + highestNodeId + " (" + 100 *((float)node / highestNodeId) + "%)");
              }

              if (!sourceDb.nodeExists(node)) continue;

              // Add the relationship ids from this node to the set of all known relationship ids.
              if (!dontStoreRelIds) {
                  for (long relId : sourceDb.getRelationshipIds(node)) {
                      relationshipIds.add(relId);
                  }
              }
              targetDb.createNode(node, getProperties(sourceDb.getNodeProperties(node), ignoreProperties), labelsArray(sourceDb, node,ignoreLabels));
            }
            catch (InvalidRecordException exp) {
              notFound += 1;
            }
        }
        System.out.println("\n copying of " + node + " nodes took " + (System.currentTimeMillis() - time) + " ms. Not found " + notFound);
        return relationshipIds;
    }

    private static void flushCache(BatchInserter sourceDb, long node) {
        Map<String, Object> nodeProperties = sourceDb.getNodeProperties(node);
        Iterator<Map.Entry<String, Object>> iterator = nodeProperties.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<String, Object> firstProp = iterator.next();
            sourceDb.nodeHasProperty(node,firstProp.getKey());
            sourceDb.setNodeProperty(node, firstProp.getKey(), firstProp.getValue()); // force flush
            System.out.print("F");
        }
    }

    private static Label[] labelsArray(BatchInserter db, long node, Set<String> ignoreLabels) {
        Collection<Label> labels = IteratorUtil.asCollection(db.getNodeLabels(node));
        if (labels.isEmpty()) return NO_LABELS;
        if (!ignoreLabels.isEmpty()) {
            for (Iterator<Label> it = labels.iterator(); it.hasNext(); ) {
                Label label = it.next();
                if (ignoreLabels.contains(label.name().toLowerCase())) {
                    it.remove();
                }
            }
        }
        return labels.toArray(new Label[labels.size()]);
    }

    private static Map<String, Object> getProperties(Map<String, Object> pc, Set<String> ignoreProperties) {
        if (!ignoreProperties.isEmpty()) pc.keySet().removeAll(ignoreProperties);
        return pc;
    }

    private static void addLog(BatchRelationship rel, String property, String message) {
        logs.append(String.format("%s.%s %s%n", rel, property, message));
    }

    private static void addLog(PropertyContainer pc, String property, String message) {
        logs.append(String.format("%s.%s %s%n", pc, property, message));
    }

    private static Iterator<Long> getRelationshipIdIterator(PrimitiveLongCollection relationshipIds, final long highestRelId) {
        if (relationshipIds == null) {
            return new Iterator<Long>() {
                private Long _value = 0L;
                public boolean hasNext() {
                    return _value <= highestRelId;
                }
                public Long next() {
                    return _value++;
                }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } else {
            final PrimitiveLongIterator pli = relationshipIds.iterator();
            return new Iterator<Long>() {
                public boolean hasNext() {
                    return pli.hasNext();
                }
                public Long next() {
                    return pli.next();
                }
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
