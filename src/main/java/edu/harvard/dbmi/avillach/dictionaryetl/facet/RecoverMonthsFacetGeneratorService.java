package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates month facets nested under the "Infected" or "Non-infected" facet,
 * depending on which infection-group facet the concept is already mapped to in the DB.
 *
 * Supported source structures in concept paths (used to extract the month number):
 * 1) Node-based final two nodes: ...\ (Inf|Infected|Noninf|Noninfected) \ <m>
 *    Pre-index months are expressed as "minus<m>" in the last node (e.g., minus3).
 * 2) Embedded in the last node: ..._<inf|infected|noninf|noninfected>_<m>
 * 3) Embedded before kit id:    ..._<m>_kit_id
 *
 * The infection group is determined solely by existing DB facet membership, not by path analysis.
 * Generated facet names: "{group} {mm}m-post index" (e.g., "Infected 09m-post index").
 * Generated facet display: "{mm}m-post index" (e.g., "09m-post index").
 */
@Service
public class RecoverMonthsFacetGeneratorService {

    private static final String RECOVER_ADULT_STUDY_ID = "phs003463";
    private static final String CONSORTIUM_CURATED_FACET_CATEGORY_NAME = "Consortium_Curated_Facets";
    private static final String RECOVER_ADULT_CURATED_FACE_NAME = "RECOVER Adult Curated";
    private static final String INFECTED_FACET_NAME = "Infected";
    private static final String NON_INFECTED_FACET_NAME = "Non-infected";

    private static final Pattern INT_PATTERN = Pattern.compile("^\\d{1,3}$");
    private static final Pattern EMBEDDED_SUFFIX = Pattern.compile("_(?:non)?(?:inf|infected)_(\\d{1,3})$", Pattern.CASE_INSENSITIVE);
    private static final Pattern MINUS_PATTERN = Pattern.compile("(?i)^minus(\\d{1,3})$");
    private static final Pattern RECOVER_ADULT_PATTERN = Pattern.compile("(?i)RECOVER_Adult$");
    private static final Pattern KIT_ID_PATTERN = Pattern.compile("_(\\d{1,3})_kit_id$", Pattern.CASE_INSENSITIVE);

    private enum InfectionGroup { INFECTED, NON_INFECTED }

    private final ConceptRepository conceptRepository;
    private final FacetCategoryService facetCategoryService;
    private final FacetService facetService;
    private final FacetRepository facetRepository;
    private final FacetConceptRepository facetConceptRepository;

    @Autowired
    public RecoverMonthsFacetGeneratorService(ConceptRepository conceptRepository,
                                              FacetCategoryService facetCategoryService,
                                              FacetService facetService,
                                              FacetRepository facetRepository,
                                              FacetConceptRepository facetConceptRepository) {
        this.conceptRepository = conceptRepository;
        this.facetCategoryService = facetCategoryService;
        this.facetService = facetService;
        this.facetRepository = facetRepository;
        this.facetConceptRepository = facetConceptRepository;
    }

    @Transactional
    public GenerateRecoverMonthsResponse generate(GenerateRecoverMonthsRequest req) {
        Optional<FacetCategoryModel> consortiumOpt = facetCategoryService.findByName(CONSORTIUM_CURATED_FACET_CATEGORY_NAME);
        Optional<FacetModel> recoverAdultOpt = facetService.findByName(RECOVER_ADULT_CURATED_FACE_NAME);
        Optional<FacetModel> infectedOpt = facetService.findByName(INFECTED_FACET_NAME);
        Optional<FacetModel> nonInfectedOpt = facetService.findByName(NON_INFECTED_FACET_NAME);

        if (consortiumOpt.isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Consortium Curated facets is missing.");
        }
        if (recoverAdultOpt.isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Recover Adult facet is missing.");
        }
        if (facetService.findFacetMetadataByFacetIDAndKey(
                recoverAdultOpt.get().getFacetId(), FacetLoaderService.KEY_EFFECTIVE_EXPRESSION_GROUPS).isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Recover adult facet metadata is missing.");
        }
        if (infectedOpt.isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Infected facet is missing.");
        }
        if (nonInfectedOpt.isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Non-infected facet is missing.");
        }

        FacetCategoryModel category = consortiumOpt.get();
        FacetModel infectedFacet = infectedOpt.get();
        FacetModel nonInfectedFacet = nonInfectedOpt.get();

        Set<Long> infectedConceptIds = loadConceptNodeIds(infectedFacet.getFacetId());
        Set<Long> nonInfectedConceptIds = loadConceptNodeIds(nonInfectedFacet.getFacetId());

        // month → group → concept_node_ids discovered via DB membership
        Map<Integer, Map<InfectionGroup, List<Long>>> monthGroupConcepts =
                discoverMonthGroupConcepts(infectedConceptIds, nonInfectedConceptIds);

        Set<String> discoveredMonths = monthGroupConcepts.keySet().stream()
                .map(String::valueOf)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (req.dryRun) {
            String message = monthGroupConcepts.isEmpty()
                    ? "No months discovered; nothing to generate."
                    : "Dry run: would generate month facets under " + INFECTED_FACET_NAME
                      + " and " + NON_INFECTED_FACET_NAME + ".";
            return new GenerateRecoverMonthsSuccessResponse(
                    message, category.getName(),
                    INFECTED_FACET_NAME + " / " + NON_INFECTED_FACET_NAME,
                    discoveredMonths, null, null);
        }

        if (monthGroupConcepts.isEmpty()) {
            return new GenerateRecoverMonthsSuccessResponse(
                    "No months discovered; nothing to load.",
                    category.getName(),
                    INFECTED_FACET_NAME + " / " + NON_INFECTED_FACET_NAME,
                    discoveredMonths, null, null);
        }

        int facetsCreated = 0;
        int facetsUpdated = 0;

        for (Map.Entry<Integer, Map<InfectionGroup, List<Long>>> monthEntry : monthGroupConcepts.entrySet()) {
            int month = monthEntry.getKey();
            for (Map.Entry<InfectionGroup, List<Long>> groupEntry : monthEntry.getValue().entrySet()) {
                InfectionGroup group = groupEntry.getKey();
                List<Long> conceptIds = groupEntry.getValue();

                String groupPrefix = group == InfectionGroup.INFECTED ? INFECTED_FACET_NAME : NON_INFECTED_FACET_NAME;
                String facetDisplay = String.format("%02dm-post index", month);
                String facetName = groupPrefix + " " + facetDisplay;
                Long parentId = group == InfectionGroup.INFECTED
                        ? infectedFacet.getFacetId()
                        : nonInfectedFacet.getFacetId();

                Optional<FacetModel> existing = facetRepository.findByName(facetName);
                FacetModel facetModel;
                if (existing.isPresent()) {
                    facetModel = existing.get();
                    facetModel.setFacetCategoryId(category.getFacetCategoryId());
                    facetModel.setDisplay(facetDisplay);
                    facetModel.setParentId(parentId);
                    facetRepository.save(facetModel);
                    facetsUpdated++;
                } else {
                    facetModel = new FacetModel(category.getFacetCategoryId(), facetName, facetDisplay, "", parentId);
                    facetRepository.save(facetModel);
                    facetsCreated++;
                }

                facetConceptRepository.deleteAllForFacetIds(List.of(facetModel.getFacetId()));
                facetConceptRepository.bulkMap(facetModel.getFacetId(), conceptIds);
            }
        }

        Result result = new Result(0, 0, facetsCreated, facetsUpdated, List.of(), List.of(), List.of());
        return new GenerateRecoverMonthsSuccessResponse(
                "Generation complete.",
                category.getName(),
                INFECTED_FACET_NAME + " / " + NON_INFECTED_FACET_NAME,
                discoveredMonths,
                result,
                null);
    }

    private Set<Long> loadConceptNodeIds(Long facetId) {
        return facetConceptRepository.findByFacetId(facetId)
                .map(list -> list.stream()
                        .map(FacetConceptModel::getConceptNodeId)
                        .collect(Collectors.toSet()))
                .orElse(Collections.emptySet());
    }

    /**
     * Streams all phs003463 concepts, extracts a month number from each path,
     * then assigns the concept to INFECTED or NON_INFECTED (or both) based on DB facet membership.
     */
    private Map<Integer, Map<InfectionGroup, List<Long>>> discoverMonthGroupConcepts(
            Set<Long> infectedIds, Set<Long> nonInfectedIds) {

        Map<Integer, Map<InfectionGroup, List<Long>>> result = new TreeMap<>();

        Stream<ConceptPathRow> rows = conceptRepository.streamDatasetNodeIdAndPath(RECOVER_ADULT_STUDY_ID);
        rows.forEach(row -> {
            String path = row.getConceptPath();
            Long conceptId = row.getConceptNodeId();

            if (RECOVER_ADULT_PATTERN.matcher(path).find()) {
                return;
            }

            Integer month = extractMonth(path);
            if (month == null) {
                return;
            }

            if (infectedIds.contains(conceptId)) {
                result.computeIfAbsent(month, k -> new LinkedHashMap<>())
                        .computeIfAbsent(InfectionGroup.INFECTED, k -> new ArrayList<>())
                        .add(conceptId);
            }
            if (nonInfectedIds.contains(conceptId)) {
                result.computeIfAbsent(month, k -> new LinkedHashMap<>())
                        .computeIfAbsent(InfectionGroup.NON_INFECTED, k -> new ArrayList<>())
                        .add(conceptId);
            }
        });

        return result;
    }

    /**
     * Extracts a month integer from a concept path, or returns null if no month is found.
     * Handles three path patterns: node-based (Inf|Noninf prefix), embedded suffix, and kit_id.
     */
    private Integer extractMonth(String path) {
        List<String> nodes = FacetExpressionEvaluator.splitConceptPath(path);
        int n = nodes.size();
        if (n == 0) {
            return null;
        }
        String last = nodes.get(n - 1);
        String prev = n >= 2 ? nodes.get(n - 2) : null;

        // Node-based: prev node is (inf|infected|noninf|noninfected), last is an integer or minus<N>
        if (prev != null && prev.matches("(?i)^(inf|infected|noninf|noninfected)$")) {
            if (INT_PATTERN.matcher(last).matches()) {
                return Integer.parseInt(last);
            }
            Matcher minusMatcher = MINUS_PATTERN.matcher(last);
            if (minusMatcher.matches()) {
                return -Integer.parseInt(minusMatcher.group(1));
            }
        }

        // Embedded suffix: last node ends with _<inf|noninf|infected|noninfected>_<digits>
        Matcher embeddedMatcher = EMBEDDED_SUFFIX.matcher(last);
        if (embeddedMatcher.find()) {
            try {
                return Integer.parseInt(embeddedMatcher.group(1));
            } catch (NumberFormatException ignored) {
            }
        }

        // Embedded before kit_id: last node ends with _<digits>_kit_id
        Matcher kitMatcher = KIT_ID_PATTERN.matcher(last);
        if (kitMatcher.find()) {
            try {
                return Integer.parseInt(kitMatcher.group(1));
            } catch (NumberFormatException ignored) {
            }
        }

        return null;
    }
}
