package edu.harvard.dbmi.avillach.dictionaryetl.Utility;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DatabaseCleanupUtility {

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void truncateTables() {
        entityManager.createNativeQuery("TRUNCATE TABLE " +
                                        " dict.concept_node_meta," +
                                        " dict.concept_node," +
                                        " dict.dataset " +
                                        "CASCADE").executeUpdate();
    }


    @Transactional
    public void truncateTablesAllTables() {
        String sql = """
                DO $$\s
                DECLARE\s
                    table_name TEXT;\s
                BEGIN\s
                    FOR table_name IN\s
                        SELECT tablename\s
                        FROM pg_tables\s
                        WHERE schemaname = 'dict' and tablename != 'update_info'
                    LOOP\s
                        EXECUTE 'TRUNCATE TABLE dict.' || table_name || ' CASCADE';\s
                    END LOOP;\s
                END $$;""";

        entityManager.createNativeQuery(sql).executeUpdate();
    }
}
