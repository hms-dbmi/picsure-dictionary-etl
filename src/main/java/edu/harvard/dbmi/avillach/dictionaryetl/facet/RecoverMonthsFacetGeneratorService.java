package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates month facets under "RECOVER Adult Curated".
 * Supported source structures in concept paths:
 * 1) Node-based final two nodes: ...\ (Inf|Infected|Noninf|Noninfected) \ <m>
 * - Pre-index months are expressed as "minus<m>" in the last node (e.g., minus3).
 * 2) Embedded in the last node: ..._<inf|infected|noninf|noninfected>_<m>
 * 3) Embedded before kit id:    ..._<m>_kit_id
 * <p>
 * Expression groups (OR) created for each month facet:
 * For m > 0:
 * - Group A (node-based):    scope + { -2: "(?i)^(inf|infected|noninf|noninfected)$", -1: exactly "<m>" }
 * - Group B (embedded last): scope + { -1: "(?i).+_(?:non)?(?:inf|infected)_<m>$" }
 * - Group C (pre-kit_id):    scope + { -1: "(?i).+_<m>_kit_id$" }
 * For m < 0:
 * - Group A- (node-based):   scope + { -2: "(?i)^(inf|infected|noninf|noninfected)$", -1: "(?i)^minus<abs(m)>$" }
 */
@Service
public class RecoverMonthsFacetGeneratorService {

    private static final String RECOVER_ADULT_STUDY_ID = "phs003463";
    private static final String CONSORTIUM_CURATED_FACET_CATEGORY_NAME = "Consortium_Curated_Facets";
    private static final String RECOVER_ADULT_CURATED_FACE_NAME = "RECOVER Adult Curated";
    private static final Pattern INT_PATTERN = Pattern.compile("^\\d{1,3}$");
    private static final Pattern EMBEDDED_SUFFIX = Pattern.compile("_(?:non)?(?:inf|infected)_(\\d{1,3})$", Pattern.CASE_INSENSITIVE);
    private static final Pattern MINUS_3_PATTERN = Pattern.compile("(?i)^minus(\\d{1,3})$");
    private static final Pattern RECOVER_ADULT_PATTERN = Pattern.compile("(?i)RECOVER_Adult$");

    private final ConceptRepository conceptRepository;
    private final FacetLoaderService facetLoaderService;
    private final FacetCategoryService facetCategoryService;
    private final FacetService facetService;

    @Autowired
    public RecoverMonthsFacetGeneratorService(ConceptRepository conceptRepository,
                                              FacetLoaderService facetLoaderService,
                                              FacetCategoryService facetCategoryService,
                                              FacetService facetService) {
        this.conceptRepository = conceptRepository;
        this.facetLoaderService = facetLoaderService;
        this.facetCategoryService = facetCategoryService;
        this.facetService = facetService;
    }

    @Transactional
    public GenerateRecoverMonthsResponse generate(GenerateRecoverMonthsRequest req) {
        Optional<FacetCategoryModel> consortiumCuratedFacets = facetCategoryService.findByName(CONSORTIUM_CURATED_FACET_CATEGORY_NAME);
        Optional<FacetModel> recoverAdultOptional = facetService.findByName(RECOVER_ADULT_CURATED_FACE_NAME);

        if (consortiumCuratedFacets.isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Consortium Curated facets is missing.");
        }

        if (recoverAdultOptional.isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Recover Adult facet is missing.");
        }

        Optional<FacetMetadataModel> facetMetadataByFacetIDAndKey = facetService.findFacetMetadataByFacetIDAndKey(recoverAdultOptional.get().getFacetId(), FacetLoaderService.KEY_EFFECTIVE_EXPRESSION_GROUPS);
        if (facetMetadataByFacetIDAndKey.isEmpty()) {
            return new GenerateRecoverMonthsErrorResponse("Recover adult facet metadata is missing.");
        }

        FacetCategoryModel facetCategoryModel = consortiumCuratedFacets.get();
        FacetModel recoverAdultFacetModel = recoverAdultOptional.get();
        String categoryName = facetCategoryModel.getName();
        String parentName = recoverAdultFacetModel.getName();
        Set<Integer> months = discoverMonths();

        String message = null;
        if (req.dryRun) {
            message = months.isEmpty() ? "No months discovered; nothing to generate." :
                    "Dry run: would generate parent facet and month facets under it.";
        }

        FacetCategoryWrapper wrapper = buildWrapper(
                categoryName, facetCategoryModel.getDisplay(), facetCategoryModel.getDescription(),
                parentName, recoverAdultFacetModel.getDisplay(), recoverAdultFacetModel.getDescription(),
                months
        );

        if (wrapper == null || wrapper.facetCategory() == null || wrapper.facetCategory().facets() == null || wrapper.facetCategory().facets().isEmpty()) {
            message = "No months discovered; nothing to load.";
        }

        Set<String> discovered = months.stream().map(Object::toString)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (StringUtils.isNotBlank(message)) {
            return new GenerateRecoverMonthsSuccessResponse(
                    message,
                    categoryName,
                    parentName,
                    discovered,
                    null,
                    null
            );
        }

        Result load = facetLoaderService.load(List.of(wrapper));
        message = "Generation complete.";
        return new GenerateRecoverMonthsSuccessResponse(
                message,
                categoryName,
                parentName,
                discovered,
                load,
                null
        );
    }

    private Set<Integer> discoverMonths() {
        Set<Integer> months = new TreeSet<>();
        Stream<ConceptPathRow> rows = conceptRepository.streamDatasetNodeIdAndPath(RECOVER_ADULT_STUDY_ID);
        rows.forEach(row -> {
            String path = row.getConceptPath();
            if (RECOVER_ADULT_PATTERN.matcher(path).find()) {
                return;
            }

            List<String> nodes = FacetExpressionEvaluator.splitConceptPath(path);
            int n = nodes.size();
            String last = nodes.get(n - 1);
            String prev = n >= 2 ? nodes.get(n - 2) : null;

            // Case 1 & 2: previous node = (Inf|Noninf) and last node = integer or last node = minus3
            if (prev != null && last != null && prev.matches("(?i)^(inf|infected|noninf|noninfected)$")) {
                if (INT_PATTERN.matcher(last).matches()) {
                    months.add(Integer.parseInt(last));
                    return;
                }

                Matcher minusMatcher = MINUS_3_PATTERN.matcher(last);
                if (minusMatcher.matches()) {
                    months.add(-Integer.parseInt(minusMatcher.group(1)));
                    return;
                }
            }

            // Case 2: last node ends with _<inf|infected|noninf|noninfected>_<digits>
            if (last != null) {
                Matcher m = EMBEDDED_SUFFIX.matcher(last);
                if (m.find()) {
                    try {
                        months.add(Integer.parseInt(m.group(1)));
                    } catch (NumberFormatException ignored) {
                        // ignore non numeric after inf/noninf
                    }
                }
            }
        });

        return months;
    }

    private FacetCategoryWrapper buildWrapper(
            String categoryName, String categoryDisplay, String categoryDescription,
            String parentName, String parentDisplay, String parentDescription,
            Set<Integer> months
    ) {
        if (months.isEmpty()) {
            return null;
        }

        // Parent scope expressions
        FacetExpressionDTO p0 = new FacetExpressionDTO(
                RECOVER_ADULT_STUDY_ID,
                null,
                null,
                0
        );

        FacetExpressionDTO p1 = new FacetExpressionDTO(
                null,
                null,
                RECOVER_ADULT_PATTERN.toString(),
                1
        );

        // Build child month facets with OR groups
        List<FacetDTO> children = months.stream().map(month -> {
            boolean isNegative = month < 0;
            String name = String.format("%02dm-post index", month);

            // Group 1: node-based (...\ (inf|noninf) \ m \)
            FacetExpressionDTO n1 = new FacetExpressionDTO(
                    null,
                    null,
                    "(?i)^(inf|infected|noninf|noninfected)$",
                    -2
            );

            String childDescription = isNegative
                    ? "RECOVER Adult concepts where the last two nodes are (Inf|Noninf)\\minus" + Math.abs(month) + "\\"
                    : "RECOVER Adult concepts where the last two nodes are (Inf|Noninf)\\" + month + "\\";

            List<List<FacetExpressionDTO>> childExpressions = new ArrayList<>();
            if (isNegative) {
                FacetExpressionDTO negativeNumberCompare = new FacetExpressionDTO(
                        null,
                        null,
                        "(?i)^minus" + Math.abs(month) + "$",
                        -1
                );
                childExpressions.add(List.of(p0, p1, n1, negativeNumberCompare));
            } else {
                FacetExpressionDTO numericComparison = new FacetExpressionDTO(
                        String.valueOf(month),
                        null,
                        null,
                        -1
                );

                // embedded in the last node (..._<inf|infected|noninf|noninfected>_<m>$)
                FacetExpressionDTO embedded = new FacetExpressionDTO(
                        null,
                        null,
                        "(?i).+_(?:non)?(?:inf|infected)_" + month + "$",
                        -1
                );

                // _kit_id embedded in the last node (..._<m>_kit_id$)
                FacetExpressionDTO embeddedPreKitId = new FacetExpressionDTO(
                        null,
                        null,
                        "(?i).+_" + month + "_kit_id$",
                        -1
                );

                childExpressions.add(List.of(p0, p1, n1, numericComparison)); // direct numeric comparison
                childExpressions.add(List.of(p0, p1, embedded)); // embedded in the variable after infected or noninfected
                childExpressions.add(List.of(p0, p1, embeddedPreKitId)); // embedded in the variable before _kit_id
            }

            return new FacetDTO(
                    name,
                    name,
                    childDescription,
                    childExpressions,
                    null
            );
        }).collect(Collectors.toList());

        FacetDTO parent = new FacetDTO(
                parentName,
                parentDisplay,
                parentDescription,
                List.of(List.of(p0, p1)),
                children
        );

        FacetCategoryDTO cat = new FacetCategoryDTO(
                categoryName,
                categoryDisplay,
                categoryDescription,
                List.of(parent)
        );

        return new FacetCategoryWrapper(cat);
    }

}