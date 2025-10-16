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

    public ClearResult clear(FacetClearRequest request) {
        long categoriesDeleted = 0;
        long facetsDeleted = 0;
        long mappingsDeleted = 0;

        if (request == null) return new ClearResult(0,0,0);

        // 1) Clear by category names
        if (request.facetCategories != null) {
            for (String catName : request.facetCategories) {
                if (catName == null || catName.isBlank()) continue;
                Optional<FacetCategoryModel> opt = facetCategoryRepository.findByName(catName);
                if (opt.isEmpty()) continue;
                FacetCategoryModel cat = opt.get();

                // count before delete
                long facetCount = facetRepository.countByFacetCategoryId(cat.getFacetCategoryId());
                long mappingCount = facetConceptRepository.countAllForCategory(cat.getFacetCategoryId());

                // delete mappings and facets for category
                facetConceptRepository.deleteAllForCategory(cat.getFacetCategoryId());
                facetRepository.deleteAllForCategory(cat.getFacetCategoryId());

                // delete category
                facetCategoryRepository.deleteById(cat.getFacetCategoryId());

                categoriesDeleted += 1;
                facetsDeleted += facetCount;
                mappingsDeleted += mappingCount;
            }
        }

        // 2) Clear by facet names (including their descendants)
        if (request.facets != null) {
            for (String facetName : request.facets) {
                if (facetName == null || facetName.isBlank()) continue;
                Optional<FacetModel> optFacet = facetRepository.findByName(facetName);
                if (optFacet.isEmpty()) continue;
                FacetModel root = optFacet.get();

                List<Long> ids = collectFacetSubtreeIds(root.getFacetId());
                if (ids.isEmpty()) continue;

                long mappingCount = facetConceptRepository.countAllForFacetIds(ids);

                // delete mappings then facets
                facetConceptRepository.deleteAllForFacetIds(ids);
                facetRepository.deleteByIds(ids);

                facetsDeleted += ids.size();
                mappingsDeleted += mappingCount;
            }
        }

        return new ClearResult(categoriesDeleted, facetsDeleted, mappingsDeleted);
    }

    private List<Long> collectFacetSubtreeIds(Long rootId) {
        java.util.ArrayList<Long> ids = new java.util.ArrayList<>();
        java.util.ArrayDeque<Long> q = new java.util.ArrayDeque<>();
        q.add(rootId);
        while (!q.isEmpty()) {
            Long id = q.removeFirst();
            ids.add(id);
            List<FacetModel> children = facetRepository.findAllByParentId(id);
            if (children != null) {
                for (FacetModel c : children) {
                    if (c != null && c.getFacetId() != null) {
                        q.addLast(c.getFacetId());
                    }
                }
            }
        }
        return ids;
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

    public record ClearResult(long categoriesDeleted, long facetsDeleted, long mappingsDeleted) {}

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
