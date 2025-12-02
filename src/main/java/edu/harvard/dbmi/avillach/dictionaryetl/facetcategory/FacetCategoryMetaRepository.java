package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Repository
public interface FacetCategoryMetaRepository extends JpaRepository<FacetCategoryMeta, Long> {

    @Query(value = "select key FROM dict.facet_category_meta group by key;", nativeQuery = true)
    List<String> getFacetCategoryMetadataKeyNames();

    @Query(value = """
            select * from dict.facet_category_meta
            where facet_category_id in (select facet_category_id from dict.facet_category);
    """, nativeQuery = true)
    List<FacetCategoryMeta> findByFacetCategoryID(Long[] facetCategoryIDs);


    @Query(value = """
            SELECT * FROM dict.facet_category_meta
            WHERE
                facet_category_meta.facet_category_id = :facetCategoryId
                AND facet_category_meta.key = :metadataKey
    """, nativeQuery = true)
    Optional<FacetCategoryMeta> findFacetCategoryMetaByCategoryId(Long facetCategoryId, String metadataKey);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.facet_category_meta WHERE facet_category_id = :facetCategoryId", nativeQuery = true)
    void deleteFacetCategoryMetaByCategoryId(Long facetCategoryId);
}
