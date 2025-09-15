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
        String sql = "DO $$ \n" +
                     "DECLARE \n" +
                     "    table_name TEXT; \n" +
                     "BEGIN \n" +
                     "    FOR table_name IN \n" +
                     "        SELECT tablename \n" +
                     "        FROM pg_tables \n" +
                     "        WHERE schemaname = 'dict' and tablename != 'update_info'\n" +
                     "    LOOP \n" +
                     "        EXECUTE 'TRUNCATE TABLE dict.' || table_name || ' CASCADE'; \n" +
                     "    END LOOP; \n" +
                     "END $$;";

        entityManager.createNativeQuery(sql).executeUpdate();
    }
}
