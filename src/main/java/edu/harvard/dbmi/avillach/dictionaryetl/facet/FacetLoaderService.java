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
import org.springframework.beans.factory.annotation.Autowired;
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

    // New: grouped metadata keys
    public static final String KEY_FACET_EXPRESSION_GROUPS = "facet_loader.expression_groups";
    protected static final String KEY_FACET_EXPRESSION_GROUPS_HASH = "facet_loader.expression_groups_sha256hex";

    public static final String KEY_EFFECTIVE_EXPRESSION_GROUPS = "facet_loader.effective_expression_groups";
    protected static final String KEY_EFFECTIVE_EXPRESSION_GROUPS_HASH = "facet_loader.effective_expression_groups_sha256hex";

    @Autowired
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
        if (request.facetCategories() != null) {
            for (String catName : request.facetCategories()) {
                if (catName == null || catName.isBlank()) continue;
                Optional<FacetCategoryModel> opt = facetCategoryRepository.findByName(catName);
                if (opt.isEmpty()) {
                    logger.warn("clear() - Did not find facet category for facet category name: {}", catName);
                    continue;
                }
                FacetCategoryModel cat = opt.get();

                int mappingsDeletedForCat = facetConceptRepository.deleteAllForCategory(cat.getFacetCategoryId());
                int facetsDeletedForCat = facetRepository.deleteAllForCategory(cat.getFacetCategoryId());
                facetCategoryRepository.deleteById(cat.getFacetCategoryId());

                categoriesDeleted += 1;
                facetsDeleted += facetsDeletedForCat;
                mappingsDeleted += mappingsDeletedForCat;
            }
        } else {
            logger.warn("clear() - Facet Category not found in request.");
        }

        // 2) Clear by facet names (including their descendants)
        if (request.facets() != null) {
            for (String facetName : request.facets()) {
                if (facetName == null || facetName.isBlank()) continue;
                Optional<FacetModel> optFacet = facetRepository.findByName(facetName);
                if (optFacet.isEmpty()) {
                    logger.warn("clear() - unable to find facet with name: {}", facetName);
                    continue;
                }

                FacetModel root = optFacet.get();
                List<Long> ids = collectFacetSubtreeIds(root.getFacetId());
                if (ids.isEmpty()) {
                    logger.info("");
                    continue;
                }

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

        // Collectors for single-pass mapping and parent rebuild across ALL categories
        List<LeafFacetSpec> leafFacets = new ArrayList<>();
        Map<Long, List<Long>> childrenByParent = new HashMap<>();
        Map<Long, Integer> depthByFacet = new HashMap<>();
        Set<Long> toClear = new HashSet<>();
        Map<Long, String> nameById = new HashMap<>();
        Map<Long, String> categoryById = new HashMap<>();

        for (FacetCategoryWrapper wrapper : payload) {
            if (wrapper == null || wrapper.facetCategory() == null) {
                logger.warn("load() - Skipping null or incomplete facet category wrapper in payload");
                continue;
            }
            FacetCategoryDTO facetCategory = wrapper.facetCategory();
            if (StringUtils.isBlank(facetCategory.name())) {
                logger.warn("load() - Skipping no name found for facet category: {}", facetCategory);
                continue;
            }

            Optional<FacetCategoryModel> optCat = facetCategoryRepository.findByName(facetCategory.name());
            FacetCategoryModel category;
            if (optCat.isPresent()) {
                category = optCat.get();
                if (StringUtils.isNotBlank(facetCategory.display())) {
                    category.setDisplay(facetCategory.display());
                }
                if (StringUtils.isNotBlank(facetCategory.description())) {
                    category.setDescription(facetCategory.description());
                }

                facetCategoryRepository.save(category);
                categoriesUpdated++;
            } else {
                category = new FacetCategoryModel(
                        facetCategory.name(),
                        StringUtils.defaultIfBlank(facetCategory.display(), facetCategory.name()),
                        StringUtils.defaultIfBlank(facetCategory.description(), "")
                );

                facetCategoryRepository.save(category);
                categoriesCreated++;
                accum.createdCategoryNames().add(category.getName());
            }

            // Recursively process facets (collect only; single-pass mapping later)
            if (facetCategory.facets() != null) {
                for (FacetDTO f : facetCategory.facets()) {
                    Counts c = upsertFacetRecursiveCollect(
                            category.getFacetCategoryId(),
                            null,
                            f,
                            category.getName(),
                            accum.createdFacetNames(),
                            leafFacets,
                            childrenByParent,
                            depthByFacet,
                            toClear,
                            nameById,
                            categoryById,
                            List.of()
                    );
                    facetsCreated += c.created();
                    facetsUpdated += c.updated();
                }
            }
        }

        // Clear mappings for facets whose effective expression groups changed/new
        facetConceptRepository.deleteAllForFacetIds(new ArrayList<>(toClear));

        // Single pass over concepts to map leaf facets
        singlePassMapLeaves(leafFacets);

        // Rebuild parent facets bottom-up from their direct children
        rebuildParentsBottomUp(childrenByParent, depthByFacet);

        // Populate mapping breakdown counts for all facets
        for (Map.Entry<Long, String> e : nameById.entrySet()) {
            Long facetId = e.getKey();
            long mappedCount = facetConceptRepository.countForFacet(facetId);
            String facetName = e.getValue();
            String categoryName = categoryById.get(facetId);
            accum.facetMappings().add(new FacetMappingBreakdown(categoryName, facetName, mappedCount));
        }

        return new Result(categoriesCreated, categoriesUpdated, facetsCreated, facetsUpdated,
                List.copyOf(accum.createdCategoryNames()), List.copyOf(accum.createdFacetNames()), List.copyOf(accum.facetMappings()));
    }

    // Collect-only recursion: upsert facets and metadata; gather leaves/structure for single-pass mapping
    private Counts upsertFacetRecursiveCollect(
            Long categoryId,
            Long parentId,
            FacetDTO facetDTO,
            String categoryName,
            List<FacetNameNested> createdCollector,
            List<LeafFacetSpec> leafFacets,
            Map<Long, List<Long>> childrenByParent,
            Map<Long, Integer> depthByFacet,
            Set<Long> toClear,
            Map<Long, String> nameById,
            Map<Long, String> categoryById,
            List<List<FacetExpressionDTO>> inheritedGroups
    ) {
        if (facetDTO == null) {
            return new Counts(0, 0);
        }

        String name = facetDTO.name();
        if (StringUtils.isBlank(name)) {
            logger.warn("upsertFacetRecursive - Facet name must not be blank. Facet: {}", facetDTO);
            return new Counts(0, 0);
        }
        String display = StringUtils.defaultIfBlank(facetDTO.display(), name);
        String description = StringUtils.defaultIfBlank(facetDTO.description(), "");

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

        List<List<FacetExpressionDTO>> parentGroups = inheritedGroups.isEmpty() ? List.of(List.of()) : inheritedGroups;

        List<List<FacetExpressionDTO>> ownGroups = facetDTO.expressionGroups() != null
                ? facetDTO.expressionGroups()
                : List.of(List.of());

        List<List<FacetExpressionDTO>> effectiveGroups = new ArrayList<>();
        for (List<FacetExpressionDTO> pg : parentGroups) {
            for (List<FacetExpressionDTO> og : ownGroups) {
                List<FacetExpressionDTO> merged = new ArrayList<>(pg);
                merged.addAll(og);
                effectiveGroups.add(merged);
            }
        }

        try {
            String ownGroupsJson = canonicalizeExpressionGroups(facetDTO.expressionGroups());
            String ownGroupsHash = sha256Hex(ownGroupsJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSION_GROUPS, ownGroupsJson);
            upsertFacetMeta(facet.getFacetId(), KEY_FACET_EXPRESSION_GROUPS_HASH, ownGroupsHash);

            // Persist effective groups and hash; collect for clear if changed
            String effectiveGroupsJson = canonicalizeExpressionGroups(effectiveGroups);
            String effectiveGroupsHash = sha256Hex(effectiveGroupsJson);
            String prevEffectiveGroupsHash = getFacetMeta(facet.getFacetId());
            boolean effectiveGroupsChanged = (prevEffectiveGroupsHash == null) || !prevEffectiveGroupsHash.equals(effectiveGroupsHash);
            if (effectiveGroupsChanged) {
                toClear.add(facet.getFacetId());
            }
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSION_GROUPS, effectiveGroupsJson);
            upsertFacetMeta(facet.getFacetId(), KEY_EFFECTIVE_EXPRESSION_GROUPS_HASH, effectiveGroupsHash);
        } catch (JsonProcessingException ex) {
            logger.warn("upsertFacetRecursive - unable to map JSON expression - {}", ex.getMessage());
        }

        // names and category for reporting later
        nameById.put(facet.getFacetId(), name);
        categoryById.put(facet.getFacetId(), categoryName);

        // depth and tree structure
        int parentDepth = depthByFacet.getOrDefault(parentId, -1);
        depthByFacet.put(facet.getFacetId(), parentDepth + 1);
        if (parentId != null) {
            childrenByParent.computeIfAbsent(parentId, k -> new ArrayList<>()).add(facet.getFacetId());
        }

        boolean hasChildren = facetDTO.facets() != null && !facetDTO.facets().isEmpty();
        if (!hasChildren) {
            // Collect leaf facet for single-pass mapping
            leafFacets.add(new LeafFacetSpec(facet.getFacetId(), effectiveGroups));
        } else {
            // Recurse into children
            for (FacetDTO child : facetDTO.facets()) {
                List<FacetNameNested> nextCollector = (createdNode != null) ? createdNode.facets : createdCollector;
                Counts c = upsertFacetRecursiveCollect(
                        categoryId,
                        facet.getFacetId(),
                        child,
                        categoryName,
                        nextCollector,
                        leafFacets,
                        childrenByParent,
                        depthByFacet,
                        toClear,
                        nameById,
                        categoryById,
                        effectiveGroups
                );
                created += c.created();
                updated += c.updated();
            }
        }

        return new Counts(created, updated);
    }


    private void singlePassMapLeaves(List<LeafFacetSpec> leafFacets) {
        if (leafFacets.isEmpty()) {
            return;
        }

        // Prepare buffers per facet
        Map<Long, List<Long>> buffers = new HashMap<>();
        final int BATCH = 1000;

        Stream<ConceptPathRow> rows = conceptRepository.streamLeafNodeIdAndPath();
        Iterator<ConceptPathRow> it = rows.iterator();
        while (it.hasNext()) {
            ConceptPathRow row = it.next();
            long conceptId = row.getConceptNodeId();
            String path = row.getConceptPath();

            for (LeafFacetSpec lf : leafFacets) {
                if (FacetExpressionEvaluator.facetAppliesToConceptPathGrouped(lf.groups(), path)) {
                    List<Long> buf = buffers.computeIfAbsent(lf.facetId(), k -> new ArrayList<>(BATCH));
                    buf.add(conceptId);
                    if (buf.size() >= BATCH) {
                        facetConceptRepository.bulkMap(lf.facetId(), buf);
                        buf.clear();
                    }
                }
            }
        }

        for (Map.Entry<Long, List<Long>> e : buffers.entrySet()) {
            if (!e.getValue().isEmpty()) {
                facetConceptRepository.bulkMap(e.getKey(), e.getValue());
            }
        }
    }

    private void rebuildParentsBottomUp(Map<Long, List<Long>> childrenByParent, Map<Long, Integer> depthByFacet) {
        if (childrenByParent.isEmpty()) {
            return;
        }

        List<Long> parents = new ArrayList<>(childrenByParent.keySet());
        parents.sort(Comparator.comparingInt(id -> depthByFacet.getOrDefault(id, 0)));
        Collections.reverse(parents); // deepest first
        for (Long parentId : parents) {
            facetConceptRepository.deleteAllForFacetIds(List.of(parentId));
            facetConceptRepository.mapParentToUnionOfDirectChildren(parentId);
        }
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

    private String getFacetMeta(Long facetId) {
        return facetMetadataRepository.findValue(facetId, FacetLoaderService.KEY_EFFECTIVE_EXPRESSION_GROUPS_HASH).orElse(null);
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(2 * dig.length);
            for (byte b : dig) {
                sb.append(String.format("%02x", b));
            }

            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }
}