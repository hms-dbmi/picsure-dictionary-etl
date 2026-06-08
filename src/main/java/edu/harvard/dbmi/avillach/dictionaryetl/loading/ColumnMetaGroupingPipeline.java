package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingContext;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import org.hibernate.query.spi.CloseableIterator;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class ColumnMetaGroupingPipeline {

    private final ColumnMetaSource columnMetaSource;
    private final ColumnMetaTreeBuilder columnMetaTreeBuilder;
    private final StudyFilter studyFilter;

    public ColumnMetaGroupingPipeline(ColumnMetaSource columnMetaSource, ColumnMetaTreeBuilder columnMetaTreeBuilder, StudyFilter studyFilter) {
        this.columnMetaSource = columnMetaSource;
        this.columnMetaTreeBuilder = columnMetaTreeBuilder;
        this.studyFilter = studyFilter;
    }

    public void run(LoadingContext context) {
        try (ExecutorService columnMetaScopeExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            String currentConcept = null;
            List<ColumnMeta> group = new ArrayList<>();
            boolean groupAllowed = true;

            try (CloseableIterator<ColumnMeta> iterator = columnMetaSource.read(context)) {
                while (iterator.hasNext()) {
                    ColumnMeta meta = iterator.next();
                    String conceptName = meta.name();

                    if (!conceptName.equals(currentConcept)) {
                        if (!group.isEmpty()) {
                            // When we clear it, the group collection passed to the thread will be cleared.
                            // Because of this we need to create a shallow copy of the file.
                            ArrayList<ColumnMeta> columnMetas = new ArrayList<>(group);
                            columnMetaScopeExecutor.submit(() -> this.columnMetaTreeBuilder.process(columnMetas, context));
                        }

                        group.clear();
                        currentConcept = conceptName;
                        groupAllowed = studyFilter.isAllowed(conceptName, context.allowedStudies());
                    }

                    if (groupAllowed) {
                        group.add(meta);
                    }
                }

                if (!group.isEmpty()) {
                    ArrayList<ColumnMeta> columnMetas = new ArrayList<>(group);
                    columnMetaScopeExecutor.submit(() -> this.columnMetaTreeBuilder.process(columnMetas, context));
                }
            }
        }
    }


}
