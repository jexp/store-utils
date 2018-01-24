package org.neo4j.tool;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.ReadOnlyIdGeneratorFactory;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.logging.NullLogProvider;

import java.io.File;
import java.io.IOException;

/**
 * @author mh
 * @since 27.05.17
 */
public class StoreCopyMigration {
    private FileSystemAbstraction fileSystem;
    private PageCache pageCache;
    private Config config;

    private NeoStores instantiateLegacyStore(RecordFormats format, File storeDir )
    {
        return new StoreFactory( storeDir, config, new ReadOnlyIdGeneratorFactory(), pageCache, fileSystem,
                format, NullLogProvider.getInstance() ).openAllNeoStores( true );
    }


    private void migrateWithBatchImporter(File storeDir, File migrationDir, long lastTxId, long lastTxChecksum,
                                          long lastTxLogVersion, long lastTxLogByteOffset, MigrationProgressMonitor.Section progressMonitor,
                                          RecordFormats oldFormat, RecordFormats newFormat )
            throws IOException
    {
/*        prepareBatchImportMigration( storeDir, migrationDir, oldFormat, newFormat );

        boolean requiresDynamicStoreMigration = !newFormat.dynamic().equals( oldFormat.dynamic() );
        boolean requiresPropertyMigration =
                !newFormat.property().equals( oldFormat.property() ) || requiresDynamicStoreMigration;
        File badFile = new File( storeDir, Configuration.BAD_FILE_NAME );
        try (NeoStores legacyStore = instantiateLegacyStore( oldFormat, storeDir );
             RecordCursors nodeInputCursors = new RecordCursors( legacyStore );
             RecordCursors relationshipInputCursors = new RecordCursors( legacyStore );
             OutputStream badOutput = new BufferedOutputStream( new FileOutputStream( badFile, false ) ) )
        {
            Configuration importConfig = new Configuration.Overridden( config );
            AdditionalInitialIds additionalInitialIds =
                    readAdditionalIds( lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset );

            // We have to make sure to keep the token ids if we're migrating properties/labels
            BatchImporter importer = new ParallelBatchImporter( migrationDir.getAbsoluteFile(), fileSystem, pageCache,
                    importConfig, logService,
                    withDynamicProcessorAssignment( migrationBatchImporterMonitor( legacyStore, progressMonitor,
                            importConfig ), importConfig ), additionalInitialIds, config, newFormat );
            InputIterable<InputNode> nodes =
                    legacyNodesAsInput( legacyStore, requiresPropertyMigration, nodeInputCursors );
            InputIterable<InputRelationship> relationships =
                    legacyRelationshipsAsInput( legacyStore, requiresPropertyMigration, relationshipInputCursors );
            importer.doImport(
                    Inputs.input( nodes, relationships, IdMappers.actual(), IdGenerators.fromInput(),
                            Collectors.badCollector( badOutput, 0 ) ) );

            // During migration the batch importer doesn't necessarily writes all entities, depending on
            // which stores needs migration. Node, relationship, relationship group stores are always written
            // anyways and cannot be avoided with the importer, but delete the store files that weren't written
            // (left empty) so that we don't overwrite those in the real store directory later.
            Collection<StoreFile> storesToDeleteFromMigratedDirectory = new ArrayList<>();
            storesToDeleteFromMigratedDirectory.add( StoreFile.NEO_STORE );
            if ( !requiresPropertyMigration )
            {
                // We didn't migrate properties, so the property stores in the migrated store are just empty/bogus
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        StoreFile.PROPERTY_STORE,
                        StoreFile.PROPERTY_STRING_STORE,
                        StoreFile.PROPERTY_ARRAY_STORE ) );
            }
            if ( !requiresDynamicStoreMigration )
            {
                // We didn't migrate labels (dynamic node labels) or any other dynamic store
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        StoreFile.NODE_LABEL_STORE,
                        StoreFile.LABEL_TOKEN_STORE,
                        StoreFile.LABEL_TOKEN_NAMES_STORE,
                        StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                        StoreFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        StoreFile.SCHEMA_STORE ) );
            }
            StoreFile.fileOperation( DELETE, fileSystem, migrationDir, null, storesToDeleteFromMigratedDirectory,
                    true, null, StoreFileType.values() );
            // When migrating on a block device there might be some files only accessible via the page cache.
            try
            {
                Predicate<FileHandle> fileHandlePredicate = fileHandle -> storesToDeleteFromMigratedDirectory.stream()
                        .anyMatch( storeFile -> storeFile.fileName( StoreFileType.STORE )
                                .equals( fileHandle.getFile().getName() ) );
                pageCache.streamFilesRecursive( migrationDir ).filter( fileHandlePredicate )
                        .forEach( FileHandle.HANDLE_DELETE );
            }
            catch ( NoSuchFileException e )
            {
                // This means that we had no files only present in the page cache, this is fine.
            }
        }
        */
    }

}
