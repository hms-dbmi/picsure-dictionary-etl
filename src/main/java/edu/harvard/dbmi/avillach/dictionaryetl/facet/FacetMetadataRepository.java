package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.List;
import java.util.Optional;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetMetadataModel;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface FacetMetadataRepository extends JpaRepository<FacetMetadataModel, Long> {
    List<FacetMetadataModel> findByFacetId(long facetId);

    Optional<FacetMetadataModel> findByFacetIdAndKey(long facetId, String key);

    @Query(value = "select key FROM dict.facet_meta group by key;", nativeQuery = true)
    List<String> getFacetMetadataKeyNames();

    @Modifying
    @Transactional
    @Query(value = """
        insert into dict.facet_meta (facet_id, key, value)
        values (:facetId, :key, :value)
        on conflict (key, facet_id) do update
           set value = excluded.value
        """, nativeQuery = true)
    void upsert(@Param("facetId") long facetId,
                @Param("key") String key,
                @Param("value") String value);

    /**
     * Find a metadata value for a specific facet and key.
     * Used by FacetLoaderService to compare expression hashes.
     */
    @Query(value = """
        SELECT value
        FROM dict.facet_meta
        WHERE facet_id = :facetId
          AND key = :key
        LIMIT 1;
        """, nativeQuery = true)
    Optional<String> findValue(@Param("facetId") Long facetId, @Param("key") String key);


}
