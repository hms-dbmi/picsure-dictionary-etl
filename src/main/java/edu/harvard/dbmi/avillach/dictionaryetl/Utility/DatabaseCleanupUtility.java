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

}
