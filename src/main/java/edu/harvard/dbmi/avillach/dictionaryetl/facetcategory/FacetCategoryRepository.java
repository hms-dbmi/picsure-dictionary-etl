package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FacetCategoryRepository extends JpaRepository<FacetCategoryModel, Long> {
    List<FacetCategoryModel> findByFacetCategoryId(long facetCategoryId);

    Optional<FacetCategoryModel> findByName(String facetCategoryName);

}
