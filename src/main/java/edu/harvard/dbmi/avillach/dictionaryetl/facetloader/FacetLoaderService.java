package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class FacetLoaderService {

    private final FacetCategoryRepository facetCategoryRepository;
    private final FacetRepository facetRepository;
    private final ConceptRepository conceptRepository;
    private final FacetConceptRepository facetConceptRepository;

    public FacetLoaderService(FacetCategoryRepository facetCategoryRepository, FacetRepository facetRepository,
                              ConceptRepository conceptRepository, FacetConceptRepository facetConceptRepository) {
        this.facetCategoryRepository = facetCategoryRepository;
        this.facetRepository = facetRepository;
        this.conceptRepository = conceptRepository;
        this.facetConceptRepository = facetConceptRepository;
    }

    public Result load(List<FacetCategoryWrapper> payload) {
        int categoriesCreated = 0;
        int categoriesUpdated = 0;
        int facetsCreated = 0;
        int facetsUpdated = 0;

        if (payload == null) return new Result(0, 0, 0, 0);

        for (FacetCategoryWrapper wrapper : payload) {
            if (wrapper == null || wrapper.facetCategory == null) continue;
            FacetCategoryDTO dto = wrapper.facetCategory;

            if (dto.name == null || dto.name.isBlank()) {
                // Skip invalid category entries
                continue;
            }

            Optional<FacetCategoryModel> optCat = facetCategoryRepository.findByName(dto.name);
            FacetCategoryModel category;
            if (optCat.isPresent()) {
                category = optCat.get();
                // Update display/description if supplied
                if (dto.display != null) category.setDisplay(dto.display);
                if (dto.description != null) category.setDescription(dto.description);
                facetCategoryRepository.save(category);
                categoriesUpdated++;
            } else {
                category = new FacetCategoryModel(dto.name, defaultStr(dto.display, dto.name), defaultStr(dto.description, ""));
                facetCategoryRepository.save(category);
                categoriesCreated++;
            }

            // Recursively process facets
            if (dto.facets != null) {
                for (FacetDTO f : dto.facets) {
                    Counts c = upsertFacetRecursive(category.getFacetCategoryId(), null, f);
                    facetsCreated += c.created;
                    facetsUpdated += c.updated;
                }
            }
        }

        return new Result(categoriesCreated, categoriesUpdated, facetsCreated, facetsUpdated);
    }

    private Counts upsertFacetRecursive(Long categoryId, Long parentId, FacetDTO facetDTO) {
        if (facetDTO == null) return new Counts(0, 0);
        String name = facetDTO.name;
        if (name == null || name.isBlank()) return new Counts(0, 0);
        String display = defaultStr(facetDTO.display, name);
        String description = defaultStr(facetDTO.description, "");

        Optional<FacetModel> optFacet = facetRepository.findByName(name);
        FacetModel facet;
        int created = 0;
        int updated = 0;
        if (optFacet.isPresent()) {
            facet = optFacet.get();
            facet.setFacetCategoryId(categoryId);
            facet.setName(name);
            facet.setDisplay(display);
            facet.setDescription(description);
            facet.setParentId(parentId);
            facetRepository.save(facet);
            updated++;
        } else {
            facet = new FacetModel(categoryId, name, display, description, parentId);
            facetRepository.save(facet);
            created++;
        }

        // Map facet to concepts based on expressions (equals, not)
        mapFacetToConcepts(facet.getFacetId(), facetDTO.expressions);

        if (facetDTO.facets != null) {
            for (FacetDTO child : facetDTO.facets) {
                Counts c = upsertFacetRecursive(categoryId, facet.getFacetId(), child);
                created += c.created;
                updated += c.updated;
            }
        }

        return new Counts(created, updated);
    }

    private static String defaultStr(String val, String def) {
        return (val == null || val.isBlank()) ? def : val;
    }

    private static String firstNonBlank(String... vals) {
        if (vals == null) return null;
        for (String v : vals) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    public record Result(int categoriesCreated, int categoriesUpdated, int facetsCreated, int facetsUpdated) {}

    private record Counts(int created, int updated) {}

    private void mapFacetToConcepts(Long facetId, List<FacetExpressionDTO> expressions) {
        if (expressions == null || expressions.isEmpty()) return;
        List<ConceptModel> concepts = conceptRepository.findAll();
        for (ConceptModel concept : concepts) {
            String path = concept.getConceptPath();
            if (FacetExpressionEvaluator.facetAppliesToConceptPath(expressions, path)) {
                if (facetConceptRepository.findByFacetIdAndConceptNodeId(facetId, concept.getConceptNodeId()).isEmpty()) {
                    facetConceptRepository.save(new FacetConceptModel(facetId, concept.getConceptNodeId()));
                }
            }
        }
    }
}
