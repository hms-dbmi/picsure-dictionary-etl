package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FacetCategoryMetaRepository extends JpaRepository<FacetCategoryMeta, Long> {

    @Query(value = "select key FROM dict.facet_category_meta group by key;", nativeQuery = true)
    List<String> getFacetCategoryMetadataKeyNames();

    @Query(value = """
            select * from dict.facet_category_meta
            where facet_category_id in (select facet_category_id from dict.facet_category);
    """, nativeQuery = true)
    List<FacetCategoryMeta> findByFacetCategoryID(Long[] facetCategoryIDs);

}
