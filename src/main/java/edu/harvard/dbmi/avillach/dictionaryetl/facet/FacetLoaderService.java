package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Stream;

/**
 * Loads facet categories and facets, persists metadata, and maps concepts.
 * Now supports OR-grouped expressions while preserving backward compatibility.
 */
@Service
public class FacetLoaderService {

    private final Logger logger = LoggerFactory.getLogger(FacetLoaderService.class);

    private final FacetCategoryRepository facetCategoryRepository;
    private final FacetRepository facetRepository;
    private final ConceptRepository conceptRepository;
    private final FacetConceptRepository facetConceptRepository;

    // Meta repositories + object mapper
    private final FacetMetadataRepository facetMetadataRepository;
    private final ObjectMapper objectMapper;

    public static final String KEY_FACET_EXPRESSIONS = "facet_loader.expressions";
    private static final String KEY_FACET_EXPRESSIONS_HASH = "facet_loader.expressions_sha256hex";

    public static final String KEY_EFFECTIVE_EXPRESSIONS = "facet_loader.effective_expressions";
    private static final String KEY_EFFECTIVE_EXPRESSIONS_HASH = "facet_loader.effective_expressions_sha256hex";

    // New: grouped metadata keys
    public static final String KEY_FACET_EXPRESSION_GROUPS = "facet_loader.expression_groups";
    private static final String KEY_FACET_EXPRESSION_GROUPS_HASH = "facet_loader.expression_groups_sha256hex";

    public static final String KEY_EFFECTIVE_EXPRESSION_GROUPS = "facet_loader.effective_expression_groups";
    private static final String KEY_EFFECTIVE_EXPRESSION_GROUPS_HASH = "facet_loader.effective_expression_groups_sha256hex";

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

    @Transactional
    public ClearResult clear(FacetClearRequest request) {
        long categoriesDeleted = 0;
        long facetsDeleted = 0;
        long mappingsDeleted = 0;

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
        } else {
            logger.warn("clear() - Facet Category not found in request.");
        }

        // 2) Clear by facet names (including their descendants)
        if (request.facets != null) {
            for (String facetName : request.facets) {
                if (facetName == null || facetName.isBlank()) continue;
                Optional<FacetModel> optFacet = facetRepository.findByName(facetName);
                if (optFacet.isEmpty()) {
                    logger.warn("clear() - unable to find facet with name: {}", facetName);
                    continue;
                }
                FacetModel root = optFacet.get();

                List<Long> ids = collectFacetSubtreeIds(root.getFacetId());
                if (ids.isEmpty()) continue;

                mappingsDeleted += facetConceptRepository.deleteAllForFacetIds(ids);
                facetsDeleted += facetRepository.deleteByIds(ids);
            }
        } else {
            logger.warn("clear() - Not facets found in request");
        }

        return new ClearResult(categoriesDeleted, facetsDeleted, mappingsDeleted);
    }

    private List<Long> collectFacetSubtreeIds(Long rootId) {
        ArrayList<Long> ids = new ArrayList<>();
        ArrayDeque<Long> q = new ArrayDeque<>();
        q.add(rootId);
        while (!q.isEmpty()) {
            Long id = q.removeFirst();
            ids.add(id);
            List<FacetModel> children = facetRepository.findAllByParentId(id);
            q.addAll(children.stream().map(FacetModel::getFacetId).toList());
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
            if (wrapper == null || wrapper.facetCategory == null) {
                logger.warn("load() - Skipping null or incomplete facet category wrapper in payload");
                continue;
            }
            FacetCategoryDTO facetCategory = wrapper.facetCategory;
            if (facetCategory.name == null || facetCategory.name.isBlank()) {
                logger.warn("load() - Skipping no name found for facet category: {}", facetCategory);
                continue;
            }

            Optional<FacetCategoryModel> optCat = facetCategoryRepository.findByName(facetCategory.name);
            FacetCategoryModel category;
            if (optCat.isPresent()) {
                category = optCat.get();
                if (facetCategory.display != null) {
                    category.setDisplay(facetCategory.display);
                }
                if (facetCategory.description != null) {
                    category.setDescription(facetCategory.description);
                }

                facetCategoryRepository.save(category);
                categoriesUpdated++;
            } else {
                category = new FacetCategoryModel(facetCategory.name, defaultStr(facetCategory.display, facetCategory.name), defaultStr(facetCategory.description, ""));
                facetCategoryRepository.save(category);
                categoriesCreated++;
                accum.createdCategoryNames().add(category.getName());
            }

            // Recursively process facets
            if (facetCategory.facets != null) {
                for (FacetDTO f : facetCategory.facets) {
                    Counts c = upsertFacetRecursive(category.getFacetCategoryId(), null, f, category.getName(), accum, accum.createdFacetNames(), false, null);
                    facetsCreated += c.created();
                    facetsUpdated += c.updated();
                }
            }
        }

        return new Result(categoriesCreated, categoriesUpdated, facetsCreated, facetsUpdated,
                List.copyOf(accum.createdCategoryNames()), List.copyOf(accum.createdFacetNames()), List.copyOf(accum.facetMappings()));
    }

    private Counts upsertFacetRecursive(
            Long categoryId,
            Long parentId,
            FacetDTO facetDTO,
            String categoryName,
            LoadAccum accum,
            List<FacetNameNested> createdCollector,
            boolean facetCleared,
            List<FacetExpressionDTO> inheritedFlat
    ) {
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

        // Compute effective expressions (AND) and effective groups (OR-of-ANDs)
        List<FacetExpressionDTO> ownFlat = (facetDTO.expressions != null && !facetDTO.expressions.isEmpty())
                ? new ArrayList<>(facetDTO.expressions)
                : List.of();

        List<List<FacetExpressionDTO>> ownGroups = (facetDTO.expressionGroups != null && !facetDTO.expressionGroups.isEmpty())
                ? facetDTO.expressionGroups
                : null;

        // Effective flat = inheritedFlat + ownFlat
        List<FacetExpressionDTO> effectiveFlat = new ArrayList<>();
        if (inheritedFlat != null && !inheritedFlat.isEmpty()) {
            effectiveFlat.addAll(inheritedFlat);
        }

        if (!ownFlat.isEmpty()) {
            effectiveFlat.addAll(ownFlat);
        }

        // Effective groups:
        // - If ownGroups present: for each group, prepend inherited scope (flat) to form an AND-group
        // - Else if no ownGroups: if effectiveFlat not empty, treat it as a single group for traceability
        List<List<FacetExpressionDTO>> effectiveGroups = new ArrayList<>();
        if (ownGroups != null) {
            for (List<FacetExpressionDTO> g : ownGroups) {
                List<FacetExpressionDTO> merged = new ArrayList<>();
                if (inheritedFlat != null && !inheritedFlat.isEmpty()) merged.addAll(inheritedFlat);
                if (g != null && !g.isEmpty()) merged.addAll(g);
                effectiveGroups.add(merged);
            }
        } else if (!effectiveFlat.isEmpty()) {
            // Legacy behavior as one group
            effectiveGroups.add(effectiveFlat);
        }

        try {
            // Persist own flat and hash
            String ownJson = canonicalizeExpressions(ownFlat);
            String ownHash = sha256Hex(ownJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSIONS, ownJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSIONS_HASH, ownHash);

            // Persist own groups and hash
            String ownGroupsJson = canonicalizeExpressionGroups(facetDTO.expressionGroups);
            String ownGroupsHash = sha256Hex(ownGroupsJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSION_GROUPS, ownGroupsJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSION_GROUPS_HASH, ownGroupsHash);

            // Persist effective flat and hash (backward compatibility)
            String effectiveJson = canonicalizeExpressions(effectiveFlat);
            String effectiveHash = sha256Hex(effectiveJson);
            String prevEffectiveHash = getFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSIONS_HASH);
            boolean effectiveChanged = (prevEffectiveHash == null) || !prevEffectiveHash.equals(effectiveHash);
            if (effectiveChanged) {
                facetConceptRepository.deleteAllForFacetIds(List.of(facet.getFacetId()));
                facetCleared = true;
            }
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSIONS, effectiveJson);
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSIONS_HASH, effectiveHash);

            // Persist effective groups and hash; clear if changed
            String effectiveGroupsJson = canonicalizeExpressionGroups(effectiveGroups);
            String effectiveGroupsHash = sha256Hex(effectiveGroupsJson);
            String prevEffectiveGroupsHash = getFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSION_GROUPS_HASH);
            boolean effectiveGroupsChanged = (prevEffectiveGroupsHash == null) || !prevEffectiveGroupsHash.equals(effectiveGroupsHash);
            if (effectiveGroupsChanged) {
                facetConceptRepository.deleteAllForFacetIds(List.of(facet.getFacetId()));
                facetCleared = true;
            }
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSION_GROUPS, effectiveGroupsJson);
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSION_GROUPS_HASH, effectiveGroupsHash);
        } catch (Exception e) {
            if (!facetCleared) {
                facetConceptRepository.deleteAllForFacetIds(List.of(facet.getFacetId()));
                facetCleared = true;
            }
        }

        long mapped;
        boolean hasChildren = facetDTO.facets != null && !facetDTO.facets.isEmpty();
        if (!hasChildren) {
            // Leaf facet: map using EFFECTIVE GROUPS if present, else EFFECTIVE flat expressions
            if (!effectiveGroups.isEmpty()) {
                mapped = mapFacetToConceptsGrouped(facet.getFacetId(), effectiveGroups);
            } else {
                mapped = mapFacetToConcepts(facet.getFacetId(), effectiveFlat);
            }
        } else {
            // Has children: process children first so they are fully mapped
            for (FacetDTO child : facetDTO.facets) {
                // If the current facet was created, nest children under it; otherwise, keep at the current level
                List<FacetNameNested> nextCollector = (createdNode != null) ? createdNode.facets : createdCollector;
                Counts c = upsertFacetRecursive(
                        categoryId,
                        facet.getFacetId(),
                        child,
                        categoryName,
                        accum,
                        nextCollector,
                        facetCleared,
                        effectiveFlat
                );
                created += c.created();
                updated += c.updated();
            }

            // Parent should only have the union of its children’s mapped concepts.
            // Always clear existing parent mappings before rebuilding from children
            facetConceptRepository.deleteAllForFacetIds(List.of(facet.getFacetId()));

            // Insert union of direct children’s concept mappings into the parent
            mapped = facetConceptRepository.mapParentToUnionOfDirectChildren(facet.getFacetId());
        }

        accum.facetMappings().add(new FacetMappingBreakdown(categoryName, name, mapped));
        return new Counts(created, updated);
    }

    /**
     * Legacy mapping by flat AND list. Implementation assumed to exist previously.
     */
    private long mapFacetToConcepts(Long facetId, List<FacetExpressionDTO> effective) {
        // Existing implementation assumed; if not present, implement a simple version here.
        // For safety, implement a streaming mapper identical in semantics to legacy evaluator.
        // Ensure a clean state before remap
        facetConceptRepository.deleteAllForFacetIds(List.of(facetId));

        long inserted = 0;
        try (Stream<ConceptPathRow> rows = conceptRepository.streamLeafNodeIdAndPath()) {
            List<Long> batch = new ArrayList<>(1024);
            for (Iterator<ConceptPathRow> it = rows.iterator(); it.hasNext(); ) {
                ConceptPathRow row = it.next();
                if (FacetExpressionEvaluator.facetAppliesToConceptPath(effective, row.getConceptPath())) {
                    batch.add(row.getConceptNodeId());
                    if (batch.size() >= 1000) {
                        inserted += facetConceptRepository.bulkMap(facetId, batch);
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) {
                inserted += facetConceptRepository.bulkMap(facetId, batch);
            }
        }
        return inserted;
    }

    /**
     * New mapping for OR-of-AND groups.
     */
    private long mapFacetToConceptsGrouped(Long facetId, List<List<FacetExpressionDTO>> groups) {
        // Strategy:
        // - Clear existing
        // - For each concept, accept if ANY group matches (OR)
        // - Bulk insert mappings
        facetConceptRepository.deleteAllForFacetIds(List.of(facetId)); // ensure clean state before remap

        long inserted = 0;
        try (Stream<ConceptPathRow> rows = conceptRepository.streamLeafNodeIdAndPath()) {
            List<Long> batch = new ArrayList<>(1024);
            for (Iterator<ConceptPathRow> it = rows.iterator(); it.hasNext(); ) {
                ConceptPathRow row = it.next();
                if (FacetExpressionEvaluator.facetAppliesToConceptPathGrouped(groups, row.getConceptPath())) {
                    batch.add(row.getConceptNodeId());
                    if (batch.size() >= 1000) {
                        inserted += facetConceptRepository.bulkMap(facetId, batch);
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) {
                inserted += facetConceptRepository.bulkMap(facetId, batch);
            }
        }

        // Return authoritative count in DB to avoid double-count concerns
        return facetConceptRepository.countAllForFacetIds(List.of(facetId));
    }

    private static String defaultStr(String val, String def) {
        return StringUtils.defaultIfBlank(val, def);
    }

    private String canonicalizeExpressions(List<FacetExpressionDTO> expressions) throws JsonProcessingException {
        if (expressions == null) {
            expressions = List.of();
        }
        return objectMapper.writeValueAsString(expressions);
    }

    private String canonicalizeExpressionGroups(List<List<FacetExpressionDTO>> groups) throws JsonProcessingException {
        if (groups == null) {
            groups = List.of();
        }
        return objectMapper.writeValueAsString(groups);
    }

    private void upsertFacetMeta(Long facetId, String key, String value) {
        facetMetadataRepository.upsert(facetId, key, value);
    }

    private String getFacetMeta(Long facetId, String key) {
        return facetMetadataRepository.findValue(facetId, key).orElse(null);
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(2 * dig.length);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}