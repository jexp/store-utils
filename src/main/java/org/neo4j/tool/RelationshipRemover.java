package org.neo4j.tool;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

/**
 * @author mh
 * @since 12.08.11
 */
public class RelationshipRemover {

    public static void main(String[] args) {
        final MyEmbeddedGraphDatabase gdb = new MyEmbeddedGraphDatabase("target/data");
        final PersistenceManager persistenceManager = gdb.getPersistenceManager();
        final long maxRelId = gdb.getPersistenceSource().getHighestPossibleIdInUse(Relationship.class);
        final NameData relationshipType = getRelationshipType(persistenceManager, "SUGGESTED");
        deleteRelationshipsOfType(gdb,persistenceManager, maxRelId, relationshipType);
        gdb.shutdown();
    }

    private static void deleteRelationshipsOfType(GraphDatabaseService gdb, PersistenceManager persistenceManager, long maxRelId, NameData relationshipType) {
        int count=0;
        Transaction tx = gdb.beginTx();
        long time = System.currentTimeMillis();
        for (int relId=0;relId<maxRelId;relId++) {
            int delta = deleteRelationship(persistenceManager,relId,relationshipType);
            if (delta==0) continue;
            count += delta;
            if ((count % 100) == 0) {
                System.out.print(".");
            }
            if ((count % 1000) == 0) {
                tx.success();
                tx.finish();
                System.out.println(" deleted "+count+" rel-id "+relId +" 1000 took "+ (System.currentTimeMillis()-time)+" ms");
                time = System.currentTimeMillis();
                if (count > 10000) return;
                tx = gdb.beginTx();
            }
        }
        tx.success();
        tx.finish();
    }

    private static int deleteRelationship(PersistenceManager persistenceManager, int relId, NameData relationshipType) {
        final RelationshipRecord relationshipData = persistenceManager.loadLightRelationship(relId);
        if (relationshipData==null) return 0;
        if (relationshipData.getType() != relationshipType.getId())  return 0;
        final ArrayMap<Integer,PropertyData> propertyData = persistenceManager.relDelete(relId);
        return 1;
    }

    private static NameData getRelationshipType(PersistenceManager persistenceManager, String name) {
        for (NameData relationshipTypeData : persistenceManager.loadAllRelationshipTypes()) {
            if (relationshipTypeData.getName().equals(name)) {
                return relationshipTypeData;
            }
        }
        return null;
    }

    private static class MyEmbeddedGraphDatabase extends EmbeddedGraphDatabase {
        public MyEmbeddedGraphDatabase(String storeDir1) {
            super(storeDir1);
        }

        public PersistenceManager getPersistenceManager() {
            return persistenceManager;
        }
    }

}
