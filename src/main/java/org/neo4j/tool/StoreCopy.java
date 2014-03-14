package org.neo4j.tool;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.util.FileUtils;
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
                "neostore.propertystore.db.mapped_memory", "300M",
                "neostore.propertystore.db.strings.mapped_memory", "1G",
                "neostore.propertystore.db.arrays.mapped_memory", "300M",
                "neostore.propertystore.db.index.keys.mapped_memory", "100M",
                "neostore.propertystore.db.index.mapped_memory", "100M",
                "cache_type", "weak"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StoryCopy source target [rel,types,to,ignore] [properties,to,ignore]");
            return;
        }
        String sourceDir = args[0];
        String targetDir = args[1];
        Set<String> ignoreRelTypes = splitOptionIfExists(args, 2);
        Set<String> ignoreProperties = splitOptionIfExists(args, 3);
        System.out.printf("Copying from %s to %s ingoring rel-types %s ignoring properties %s %n", sourceDir, targetDir, ignoreRelTypes, ignoreProperties);
        copyStore(sourceDir, targetDir, ignoreRelTypes, ignoreProperties);
    }

    private static Set<String> splitOptionIfExists(String[] args, final int index) {
        if (args.length <= index) return emptySet();
        return new HashSet<String>(asList(args[index].toLowerCase().split(",")));
    }

    private static void copyStore(String sourceDir, String targetDir, Set<String> ignoreRelTypes, Set<String> ignoreProperties) throws Exception {
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

        copyNodes(sourceDb, targetDb, ignoreProperties, highestIds.first());
        copyRelationships(sourceDb, targetDb, ignoreRelTypes, ignoreProperties, highestIds.other());

        targetDb.shutdown();
        sourceDb.shutdown();
        logs.close();
        copyIndex(source, target);
    }

    private static Pair<Long, Long> getHighestNodeId(File source) {
        GraphDatabaseAPI api = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(source.getAbsolutePath());
        NodeManager nodeManager = api.getDependencyResolver().resolveDependency(NodeManager.class);
        long highestNodeId = nodeManager.getHighestPossibleIdInUse(Node.class);
        long highestRelId = nodeManager.getHighestPossibleIdInUse(Relationship.class);
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

    private static void copyRelationships(BatchInserter sourceDb, BatchInserter targetDb, Set<String> ignoreRelTypes, Set<String> ignoreProperties, long highestRelId) {
        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        while (relId <= highestRelId) {
            BatchRelationship rel = null;
            try {
                rel = sourceDb.getRelationshipById(relId++);
            } catch (InvalidRecordException nfe) {
                notFound++;
                continue;
            }
            if (ignoreRelTypes.contains(rel.getType().name().toLowerCase())) continue;
            createRelationship(targetDb, sourceDb, rel, ignoreProperties);
            if (relId % 1000 == 0) System.out.print(".");
            if (relId % 100000 == 0) System.out.println(" " + rel);
        }
        System.out.println("\n copying of "+relId+" relationships took "+(System.currentTimeMillis()-time)+" ms. Not found "+notFound);
    }

    private static void createRelationship(BatchInserter targetDb, BatchInserter sourceDb, BatchRelationship rel, Set<String> ignoreProperties) {
        long startNodeId = rel.getStartNode();
        long endNodeId = rel.getEndNode();
        final RelationshipType type = rel.getType();
        try {
            targetDb.createRelationship(startNodeId, endNodeId, type, getProperties(sourceDb.getRelationshipProperties(rel.getId()), ignoreProperties));
        } catch (InvalidRecordException ire) {
            addLog(rel, "create Relationship: " + startNodeId + "-[:" + type + "]" + "->" + endNodeId, ire.getMessage());
        }
    }

    private static void copyNodes(BatchInserter sourceDb, BatchInserter targetDb, Set<String> ignoreProperties, long highestNodeId) {
        long time = System.currentTimeMillis();
        int node = 0;
        while (node <= highestNodeId) {
            if (!sourceDb.nodeExists(node)) continue;
            targetDb.createNode(node, getProperties(sourceDb.getNodeProperties(node), ignoreProperties), labelsArray(sourceDb, node));
            node++;
            if (node % 1000 == 0) System.out.print(".");
            if (node % 100000 == 0) {
                logs.flush();
                System.out.println(" " + node);
            }
        }
        System.out.println("\n copying of " + node + " nodes took " + (System.currentTimeMillis() - time) + " ms.");
    }

    private static Label[] labelsArray(BatchInserter db, long node) {
        Collection<Label> labels = IteratorUtil.asCollection(db.getNodeLabels(node));
        if (labels.isEmpty()) return NO_LABELS;
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
}
