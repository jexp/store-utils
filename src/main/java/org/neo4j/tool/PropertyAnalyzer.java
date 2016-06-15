package org.neo4j.tool;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;

import java.io.File;
import java.lang.reflect.Array;
import java.util.*;

/**
 * @author mh
 * @since 28.07.11
 */
public class PropertyAnalyzer {
    static class PropertyInfo {
        String name;
        int count;
        int emptyCount;
        Set<String> types=new HashSet<String>();
        long min, max;
        int minCount, maxCount;
        PropertyInfo(String name) {
            this.name = name;
        }
        public void update(PropertyContainer pc) {
            count++;
            final Object value = pc.getProperty(name);

            final Class<?> type = value.getClass();
            types.add(type.getSimpleName());
            final int size = toSize(value);
            minCount = Math.min(minCount, size);
            maxCount = Math.max(maxCount, size);
            final long length = toNumber(value);
            min = Math.min(min, length);
            max = Math.max(max, length);
            if (isDefaultValue(value)) {
                emptyCount++;
            }
        }

        private long toNumber(Object value) {
            if (value instanceof Number) {
                return ((Number)value).longValue();
            }
            if (value instanceof String) {
                return ((String) value).length();
            }
            if (value instanceof Boolean && ((Boolean)value)) {
                return 1;
            }
            if (value.getClass().isArray()) {
                long sum=0;
                for (int i = Array.getLength(value)-1;i>=0;i--) {
                    sum += toNumber(Array.get(value,i));
                }
                return sum;
            }
            return 0;
        }

        private int toSize(Object value) {
            if (value.getClass().isArray()) {
                return Array.getLength(value);
            }
            return 1;
        }

        @Override
        public String toString() {
            return ""+name+"	"+ count+"	"+ emptyCount+"	"+ types+"	"+minCount+"	"+maxCount+"	"+min+"	"+max;
        }

        public int getEmptyCount() {
            return emptyCount;
        }

        public int getCount() {
            return count;
        }
    }

    public static Map<String,String> config() {
        return (Map) MapUtil.map(
                "neostore.nodestore.db.mapped_memory", "100M",
                "neostore.relationshipstore.db.mapped_memory", "500M",
                "neostore.propertystore.db.mapped_memory", "300M",
                "neostore.propertystore.db.strings.mapped_memory", "1G",
                "neostore.propertystore.db.arrays.mapped_memory", "300M",
                "neostore.propertystore.db.index.keys.mapped_memory", "100M",
                "neostore.propertystore.db.index.mapped_memory", "100M",
                "allow_store_upgrade","true",
                "cache_type", "weak"
        );
    }

    public static void main(String[] args) {
        final GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(args[0])).setConfig(config()).newGraphDatabase();
        int withoutProps=0, nodes = 0, rels = 0;
        Map<String,PropertyInfo> props=new HashMap<String, PropertyInfo>();
        for (Node node : db.getAllNodes()) {
            nodes ++;
            withoutProps += analyzeProperties(props, node);
            for (Relationship relationship : node.getRelationships(Direction.OUTGOING)) {
               rels ++;
               withoutProps += analyzeProperties(props, relationship);
            }
            if (nodes % 1000 == 0) {
                System.out.print(".");
                if (nodes % 100000 == 0)
                    System.out.println(" "+nodes);
            }
        }
        db.shutdown();
        outputEmptyCounts(withoutProps, props, nodes, rels);
    }

    private static int analyzeProperties(Map<String, PropertyInfo> props, PropertyContainer propertyContainer) {
        boolean hasProps = false;
        for (String property : propertyContainer.getPropertyKeys()) {
            hasProps=true;
            update(props, property,propertyContainer);
        }
        return hasProps ? 0 : 1;
    }

    private static void update(Map<String, PropertyInfo> props, String property, PropertyContainer pc) {
        PropertyInfo info = props.get(property);
        if (info==null) {
            info = new PropertyInfo(property);
            props.put(property, info);
        }
        info.update(pc);
    }

    private static void outputEmptyCounts(int withoutProps, Map<String, PropertyInfo> props, int nodes, int rels) {
        System.out.println();
        int emptyCount=0, allCount = 0;
        for (PropertyInfo info : props.values()) {
            emptyCount += info.getEmptyCount();
            allCount += info.getCount();
            System.out.println(info);
        }
        System.out.printf("%d of %d empty properties %d nodes %d rels pc w/o props %d%n", emptyCount, allCount,nodes,rels,withoutProps);
    }

    private static boolean isDefaultValue(Object property) {
        if (property==null) return true;
        if (property instanceof String) return ((String)property).isEmpty();
        if (property instanceof Number) return ((Number)property).longValue() == 0 || ((Number)property).doubleValue() == 0.0D;
        if (property instanceof Character) return (Character) property == 0;
        if (property instanceof Boolean) return !(Boolean) property;
        if (property instanceof String[]) return ((String[])property).length==0;
        if (property instanceof int[]) return ((int[])property).length==0;
        if (property instanceof long[]) return ((long[])property).length==0;
        if (property instanceof boolean[]) return ((boolean[])property).length==0;
        if (property instanceof char[]) return ((char[])property).length==0;
        if (property instanceof byte[]) return ((byte[])property).length==0;
        if (property instanceof float[]) return ((float[])property).length==0;
        if (property instanceof double[]) return ((double[])property).length==0;
        System.out.println("property = " + property);
        return false;
    }
}
