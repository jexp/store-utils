package org.neo4j.tool;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

/**
 * @author mh
 * @since 27.05.17
 */
class StoreCopyConfig {
    private static final String CONFIG_FILE = "neo4j.properties";
    public final String sourceDir;
    public final String targetDir;
    public final Set<String> ignoreRelTypes;
    public final Set<String> ignoreProperties;
    public final Set<String> ignoreLabels;
    public final Set<String> deleteNodesWithLabels;
    public final boolean keepNodeIds;
    public final String badEntriesLogDir;
    public final String storeCopyLogDir;

    public StoreCopyConfig(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(CONFIG_FILE));
        System.err.println("Read from " + CONFIG_FILE);
        System.err.println(properties);

        sourceDir = getArgument(args, 0, properties, "source_db_dir");
        targetDir = getArgument(args, 1, properties, "target_db_dir");

        ignoreRelTypes = splitToSet(getArgument(args, 2, properties, "rel_types_to_ignore"));
        ignoreProperties = splitToSet(getArgument(args, 3, properties, "properties_to_ignore"));
        ignoreLabels = splitToSet(getArgument(args, 4, properties, "labels_to_ignore"));
        deleteNodesWithLabels = splitToSet(getArgument(args, 5, properties, "labels_to_delete"));
        String keepNodeIdsParam = getArgument(args, 6, properties, "keep_node_ids");
        keepNodeIds = !("false".equalsIgnoreCase(keepNodeIdsParam));

        badEntriesLogDir = getProperty(properties, "bad_entries_log_dir", sourceDir);
        storeCopyLogDir = getProperty(properties, "store_copy_log_dir", sourceDir);
        System.err.println(this);
    }

    private String getProperty(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key, "");
        return isNullOrEmpty(value) ? defaultValue : value;
    }

    private static String getArgument(String[] args, int index, Properties properties, String key) {
        if (args.length > index) return args[index];
        return properties.getProperty(key);
    }

    private static Set<String> splitToSet(String value) {
        if (value == null || value.trim().isEmpty()) return emptySet();
        return new HashSet<>(asList(value.trim().split(", *")));
    }


    @Override
    public String toString() {
        return String.format("Copying from %s to %s ignoring rel-types %s ignoring properties %s ignoring labels %s removing nodes with labels %s keep node ids %s %n", sourceDir, targetDir, ignoreRelTypes, ignoreProperties, ignoreLabels, deleteNodesWithLabels, keepNodeIds);
    }

    public boolean isValid() {
        boolean result = true;
        if (isNullOrEmpty(sourceDir)) {
            System.err.println("Source database directory missing");
            result = false;
        }
        if (isNullOrEmpty(targetDir)) {
            System.err.println("Target database directory missing");
            result = false;
        }
        return result;
    }

    private boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }
}
