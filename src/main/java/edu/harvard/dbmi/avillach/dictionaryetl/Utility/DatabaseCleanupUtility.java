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
        entityManager.createNativeQuery("""
            TRUNCATE TABLE\s
                dict.concept_node_meta,
                dict.concept_node,
                dict.dataset\s
            CASCADE).executeUpdate();
            """);
    }


    @Transactional
    public void truncateTablesAllTables() {
        String sql =
            """
                DO $$
                DECLARE
                    table_name TEXT;
                BEGIN
                    FOR table_name IN
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'dict'
                           AND tablename NOT IN ('update_info')
                    LOOP
                        EXECUTE 'TRUNCATE TABLE dict.' || table_name || ' CASCADE';
                    END LOOP;
                END $$;;
            """;

        entityManager.createNativeQuery(sql).executeUpdate();
    }
}
