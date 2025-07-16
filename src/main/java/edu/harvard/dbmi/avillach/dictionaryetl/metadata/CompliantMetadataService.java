package edu.harvard.dbmi.avillach.dictionaryetl.metadata;

import edu.harvard.dbmi.avillach.dictionaryetl.columnmeta.AbstractColumnMetaProcessor;
import edu.harvard.dbmi.avillach.dictionaryetl.columnmeta.ColumnMetaMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class CompliantMetadataService extends AbstractColumnMetaProcessor {

    @Autowired
    public CompliantMetadataService(ColumnMetaMapper columnMetaMapper) throws SQLException {
        super(columnMetaMapper);
    }

    @Override
    protected void startProcessing() {
        // TODO: Implement compliant dictionary processing
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected ExecutorService createThreadPool() {
        // Because there are no database interactions, we can use all available processors
        return fixedThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }
}
