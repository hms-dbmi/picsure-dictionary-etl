package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FacetCategoryRepository extends JpaRepository<FacetCategoryModel, Long> {

    Optional<FacetCategoryModel> findByName(String facetCategoryName);

}
