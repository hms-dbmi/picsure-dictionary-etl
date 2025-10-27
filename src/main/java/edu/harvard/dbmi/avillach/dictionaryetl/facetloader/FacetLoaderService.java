package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static edu.harvard.dbmi.avillach.dictionaryetl.facetloader.FacetExpressionEvaluator.applies;

@Service
public class FacetLoaderService {

    private final FacetCategoryRepository facetCategoryRepository;
    private final FacetRepository facetRepository;
    private final ConceptRepository conceptRepository;
    private final FacetConceptRepository facetConceptRepository;

    // Meta repositories + object mapper
    private final FacetMetadataRepository facetMetadataRepository;
    private final ObjectMapper objectMapper;

    private static final String KEY_FACET_EXPRESSIONS = "facet_loader.expressions";
    private static final String KEY_FACET_EXPRESSIONS_HASH = "facet_loader.expressions_sha256hex";
    private static final String KEY_EFFECTIVE_EXPRESSIONS = "facet_loader.effective_expressions";
    private static final String KEY_EFFECTIVE_EXPRESSIONS_HASH = "facet_loader.effective_expressions_sha256hex";

    public FacetLoaderService(FacetCategoryRepository facetCategoryRepository, FacetRepository facetRepository,
                              ConceptRepository conceptRepository, FacetConceptRepository facetConceptRepository,
                              FacetMetadataRepository facetMetadataRepository,
                              ObjectMapper objectMapper) {
        this.facetCategoryRepository = facetCategoryRepository;
        this.facetRepository = facetRepository;
        this.conceptRepository = conceptRepository;
        this.facetConceptRepository = facetConceptRepository;
        this.facetMetadataRepository = facetMetadataRepository;
        this.objectMapper = objectMapper.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
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

    @Transactional
    public Result load(List<FacetCategoryWrapper> payload) {
        int categoriesCreated = 0;
        int categoriesUpdated = 0;
        int facetsCreated = 0;
        int facetsUpdated = 0;

        LoadAccum accum = new LoadAccum(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

        if (payload == null) return new Result(0, 0, 0, 0, List.of(), List.of(), List.of());

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
                accum.createdCategoryNames().add(category.getName());
            }

            // Recursively process facets
            if (dto.facets != null) {
                for (FacetDTO f : dto.facets) {
                    Counts c = upsertFacetRecursive(category.getFacetCategoryId(), null, f, category.getName(), accum, accum.createdFacetNames(), false, null);
                    facetsCreated += c.created();
                    facetsUpdated += c.updated();
                }
            }
        }

        return new Result(categoriesCreated, categoriesUpdated, facetsCreated, facetsUpdated,
                List.copyOf(accum.createdCategoryNames()), List.copyOf(accum.createdFacetNames()), List.copyOf(accum.facetMappings()));
    }

    private Counts upsertFacetRecursive(Long categoryId, Long parentId, FacetDTO facetDTO, String categoryName, LoadAccum accum, List<FacetNameNested> createdCollector, boolean facetCleared, List<FacetExpressionDTO> inherited) {
        if (facetDTO == null) return new Counts(0, 0);
        String name = facetDTO.name;
        if (name == null || name.isBlank()) return new Counts(0, 0);
        String display = defaultStr(facetDTO.display, name);
        String description = defaultStr(facetDTO.description, "");

        Optional<FacetModel> optFacet = facetRepository.findByName(name);
        FacetModel facet;
        int created = 0;
        int updated = 0;
        FacetNameNested createdNode = null;
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
            createdNode = new FacetNameNested(name);
            createdCollector.add(createdNode);
        }

        // Compute effective expressions = inherited + own (AND semantics when evaluated)
        List<FacetExpressionDTO> effective = new ArrayList<>();
        if (inherited != null && !inherited.isEmpty()) {
            effective.addAll(inherited);
        }
        if (facetDTO.expressions != null && !facetDTO.expressions.isEmpty()) {
            effective.addAll(facetDTO.expressions);
        }

        try {
            // Persist own expressions and hash (for traceability)
            String ownJson = canonicalizeExpressions(facetDTO.expressions);
            String ownHash = sha256Hex(ownJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSIONS, ownJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSIONS_HASH, ownHash);

            // Persist effective expressions and hash; clear mappings if changed or now empty
            String effectiveJson = canonicalizeExpressions(effective);
            String effectiveHash = sha256Hex(effectiveJson);
            String prevEffectiveHash = getFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSIONS_HASH);
            boolean effectiveChanged = (prevEffectiveHash == null) || !prevEffectiveHash.equals(effectiveHash);
            if (effectiveChanged || effective.isEmpty()) {
                facetConceptRepository.deleteAllForFacetIds(java.util.List.of(facet.getFacetId()));
                facetCleared = true;
            }
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSIONS, effectiveJson);
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSIONS_HASH, effectiveHash);
        } catch (Exception e) {
            if (!facetCleared) {
                facetConceptRepository.deleteAllForFacetIds(java.util.List.of(facet.getFacetId()));
                facetCleared = true;
            }
        }

        // Map using the EFFECTIVE expressions (parent AND child)
        long mapped = mapFacetToConcepts(facet.getFacetId(), effective);
        accum.facetMappings().add(new FacetMappingBreakdown(categoryName, name, mapped));

        if (facetDTO.facets != null) {
            for (FacetDTO child : facetDTO.facets) {
                // If the current facet was created, nest children under it; otherwise, keep at the current level
                List<FacetNameNested> nextCollector = (createdNode != null) ? createdNode.facets : createdCollector;
                Counts c = upsertFacetRecursive(categoryId, facet.getFacetId(), child, categoryName, accum, nextCollector, facetCleared, effective);
                created += c.created();
                updated += c.updated();
            }
        }

        return new Counts(created, updated);
    }

    private static String defaultStr(String val, String def) {
        return (val == null || val.isBlank()) ? def : val;
    }

    private String canonicalizeExpressions(List<FacetExpressionDTO> expressions) throws JsonProcessingException {
        if (expressions == null) return "[]";
        return objectMapper.writeValueAsString(expressions);
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void upsertFacetMeta(long facetId, String key, String value) {
        // Database-level UPSERT to avoid unique-constraint violations under concurrency
        facetMetadataRepository.upsert(facetId, key, value);
    }

    private String getFacetMeta(long facetId, String key) {
        return facetMetadataRepository.findByFacetIdAndKey(facetId, key)
                .map(FacetMetadataModel::getValue)
                .orElse(null);
    }

    @Transactional(readOnly = true)
    public long mapFacetToConcepts(Long facetId, List<FacetExpressionDTO> expressions) {
        if (expressions == null || expressions.isEmpty()) return 0L;

        long before = facetConceptRepository.countForFacet(facetId);

        // Precompile all expressions
        List<CompiledExpr> compiledExprs = expressions.stream()
                .map(CompiledExpr::new).toList();

        try(Stream<ConceptPathRow> conceptPathRowStream = conceptRepository.streamNodeIdAndPath()) {
            final int BATCH = 1000; // This can be tuned
            ArrayList<Long> buffer = new ArrayList<>(BATCH);

            conceptPathRowStream.forEach(row -> {
                if (applies(compiledExprs, row.getConceptPath())) {
                    buffer.add(row.getConceptNodeId());
                    if(buffer.size() >= BATCH) {
                        facetConceptRepository.bulkMapFacetToConceptNodes(facetId, buffer.toArray(Long[]::new));
                        buffer.clear();
                    }
                }
            });

            if(!buffer.isEmpty()) {
                facetConceptRepository.bulkMapFacetToConceptNodes(facetId, buffer.toArray(Long[]::new));
            }
        }

        long after = facetConceptRepository.countForFacet(facetId);
        long delta = after - before;
        return (delta < 0) ? 0L : delta;
    }

}
