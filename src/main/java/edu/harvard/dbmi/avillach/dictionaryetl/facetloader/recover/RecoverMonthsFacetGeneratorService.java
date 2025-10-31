package edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.FacetLoaderService;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Generates month facets under "RECOVER Adult Curated".
 * Supports both structures:
 *   1) ...\ (Inf|Noninf) \ <month>
 *   2) ..._<inf|infected|noninf|noninfected>_<month>  (at end of last node)
 *
 * Each child facet uses OR groups:
 *   Group 1 (node-based):    scope + { -2: "(?i)^(inf|noninf)$", -1: exactly "<m>" }
 *   Group 2 (embedded last): scope + { -1: "(?i)_(?:non)?(?:inf|infected)_<m>$" }
 */
@Service
public class RecoverMonthsFacetGeneratorService {

    private static final Pattern INT_PATTERN = Pattern.compile("^\\d{1,3}$");
    private static final Pattern EMBEDDED_SUFFIX = Pattern.compile("_(?:non)?(?:inf|infected)_(\\d{1,3})$", Pattern.CASE_INSENSITIVE);
    private static final Pattern MINUS_3_PATTERN = Pattern.compile("(?i)^minus(\\d{1,3})$");

    private final ConceptRepository conceptRepository;
    private final FacetLoaderService facetLoaderService;
    private final FacetCategoryService facetCategoryService;
    private final FacetService facetService;

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
        GenerateRecoverMonthsResponse out = new GenerateRecoverMonthsResponse();
        Optional<FacetCategoryModel> consortiumCuratedFacets = facetCategoryService.findByName("Consortium_Curated_Facets");
        Optional<FacetModel> recoverAdultOptional = facetService.findByName("RECOVER Adult Curated");
        if (consortiumCuratedFacets.isPresent() && recoverAdultOptional.isPresent()) {
            Optional<FacetMetadataModel> facetMetadataByFacetIDAndKey =
                    facetService.findFacetMetadataByFacetIDAndKey(recoverAdultOptional.get().getFacetId(), FacetLoaderService.KEY_EFFECTIVE_EXPRESSIONS);
            if (facetMetadataByFacetIDAndKey.isPresent()) {
                FacetCategoryModel facetCategoryModel = consortiumCuratedFacets.get();
                FacetModel recoverAdultFacetModel = recoverAdultOptional.get();
                String categoryName = facetCategoryModel.getName();
                String parentName = recoverAdultFacetModel.getName();

                // RECOVER Adult scoping expressions (copied into children)
                String studyId = defaultStr(req.studyId, "phs003463");
                String adultNodeRegex = defaultStr(req.adultNodeRegex, "(?i)RECOVER_Adult$");

                // Discover months present in the repository (optionally scoped for discovery only)
                Set<Integer> months = discoverMonths(req.pathPrefixRegex);

                out.categoryName = categoryName;
                out.parentFacetName = parentName;
                out.discoveredMonths = months.stream().map(Object::toString)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

                if (Boolean.TRUE.equals(req.dryRun)) {
                    out.message = months.isEmpty() ? "No months discovered; nothing to generate." :
                            "Dry run: would generate parent facet and month facets under it.";
                    return out;
                }

                // Optional clear: entire category or just the parent subtree
                // TODO: Remove optional clear.
                if (Boolean.TRUE.equals(req.clearCategoryFirst)) {
                    FacetClearRequest clr = new FacetClearRequest();
                    clr.facetCategories = List.of(categoryName);
                    out.clear = facetLoaderService.clear(clr);
                } else if (Boolean.TRUE.equals(req.clearParentFacetFirst)) {
                    FacetClearRequest clr = new FacetClearRequest();
                    clr.facets = List.of(parentName);
                    out.clear = facetLoaderService.clear(clr);
                }

                FacetCategoryWrapper wrapper = buildWrapper(
                        categoryName, facetCategoryModel.getDisplay(), facetCategoryModel.getDescription(),
                        parentName, recoverAdultFacetModel.getDisplay(), recoverAdultFacetModel.getDescription(),
                        studyId, adultNodeRegex, months
                );

                if (wrapper == null || wrapper.facetCategory == null || wrapper.facetCategory.facets == null || wrapper.facetCategory.facets.isEmpty()) {
                    out.message = "No months discovered; nothing to load.";
                    return out;
                }

                out.load = facetLoaderService.load(List.of(wrapper));
                out.message = "Generation complete.";
            } else {
                out.message = "Recover adult facet metadata is missing.";
            }

        } else {
            boolean consortiumFacetMissing = consortiumCuratedFacets.isEmpty();
            boolean recoverAdultFacetMissing = recoverAdultOptional.isEmpty();

            if (consortiumFacetMissing && recoverAdultFacetMissing) {
                out.message = "Consortium Curated facet and Recover adult facet is missing.";
            } else if (consortiumFacetMissing) {
                out.message = "Consortium Curated facets is missing.";
            } else {
                out.message = "Recover Adult facet is missing.";
            }
        }

        return out;
    }

    private Set<Integer> discoverMonths(String pathPrefixRegex) {
        Set<Integer> months = new TreeSet<>();
        final Pattern compiledPrefix = compilePrefix(pathPrefixRegex);

        try (Stream<ConceptPathRow> rows = conceptRepository.streamNodeIdAndPath()) {
            rows.forEach(row -> {
                String path = row.getConceptPath();
                if (compiledPrefix != null && (path == null || !compiledPrefix.matcher(path).find())) return;

                List<String> nodes = split(path);
                int n = nodes.size();
                if (n == 0) return;

                String last = nodes.get(n - 1);
                String prev = n >= 2 ? nodes.get(n - 2) : null;

                // Case 1 & 2: previous node = (Inf|Noninf) and last node = integer or last node = minus3
                if (prev != null && last != null
                    && prev.matches("(?i)^(inf|infected|noninf|noninfected)$")) {
                    if(INT_PATTERN.matcher(last).matches()) {
                        try {
                            months.add(Integer.parseInt(last));
                            return;
                        } catch (NumberFormatException ignored) {
                            // ignore non-numeric tails
                        }
                    }

                    Matcher minusMatcher = MINUS_3_PATTERN.matcher(last);
                    if (minusMatcher.matches()) {
                        months.add(-Integer.parseInt(minusMatcher.group(1)));
                        return;
                    }
                }

                // Case 2: last node ends with _<inf|infected|noninf|noninfected>_<digits>
                if (last != null) {
                    java.util.regex.Matcher m = EMBEDDED_SUFFIX.matcher(last);
                    if (m.find()) {
                        try {
                            months.add(Integer.parseInt(m.group(1)));
                        } catch (NumberFormatException ignored) {
                            // ignore non numeric after inf/noninf
                        }
                    }
                }
            });
        }
        return months;
    }

    private static Pattern compilePrefix(String pathPrefixRegex) {
        if (pathPrefixRegex == null || pathPrefixRegex.isBlank()) return null;
        try {
            return Pattern.compile(pathPrefixRegex, Pattern.CASE_INSENSITIVE);
        } catch (Exception e) {
            // Invalid regex provided; ignore and match all
            return null;
        }
    }

    private FacetCategoryWrapper buildWrapper(
            String categoryName, String categoryDisplay, String categoryDescription,
            String parentName, String parentDisplay, String parentDescription,
            String studyId, String adultNodeRegex, Set<Integer> months
    ) {
        if (months == null || months.isEmpty()) return null;

        // Parent scope expressions
        FacetExpressionDTO p0 = new FacetExpressionDTO();
        p0.exactly = studyId;
        p0.node = 0;

        FacetExpressionDTO p1 = new FacetExpressionDTO();
        p1.regex = adultNodeRegex;
        p1.node = 1;

        // Build child month facets with OR groups
        List<FacetDTO> children = months.stream().map(month -> {
            boolean isNegative = month < 0;
            String name = String.format("%02dm-post index", month);

            // Group 1: node-based (...\ (inf|noninf) \ m \)
            FacetExpressionDTO n1 = new FacetExpressionDTO();
            n1.regex = "(?i)^(inf|infected|noninf|noninfected)$";
            n1.node = -2;

            String childDescription = isNegative
                    ? "RECOVER Adult concepts where the last two nodes are (Inf|Noninf)\\minus" + Math.abs(month) + "\\"
                    : "RECOVER Adult concepts where the last two nodes are (Inf|Noninf)\\" + month + "\\";

            FacetDTO child = new FacetDTO();
            child.name = name;
            child.display = name;
            child.description = childDescription;

            List<List<FacetExpressionDTO>> childExpressions = new ArrayList<>();
            FacetExpressionDTO numericComparison = new FacetExpressionDTO();
            if (isNegative) {
                numericComparison.node = -1;
                numericComparison.regex = "(?i)^minus" + Math.abs(month) + "$";
                childExpressions.add(List.of(p0, p1, n1, numericComparison));
            } else {
                numericComparison.exactly = String.valueOf(month);
                numericComparison.node = -1;

                // embedded in the last node (..._<inf|infected|noninf|noninfected>_<m>$)
                FacetExpressionDTO embedded = new FacetExpressionDTO();
                embedded.regex = "(?i).+_(?:non)?(?:inf|infected)_" + month + "$";
                embedded.node = -1;

                // _kit_id embedded in the last node (..._<m>_kit_id$)
                FacetExpressionDTO embeddedPreKitId = new FacetExpressionDTO();
                embeddedPreKitId.regex = "(?i).+_" + month + "_kit_id$";
                embeddedPreKitId.node = -1;

                childExpressions.add(List.of(p0, p1, n1, numericComparison)); // direct numeric comparison
                childExpressions.add(List.of(p0, p1, embedded)); // embedded in the variable after infected or noninfected
                childExpressions.add(List.of(p0, p1, embeddedPreKitId)); // embedded in the variable before _kit_id
            }

            child.expressionGroups = childExpressions;
            return child;
        }).collect(Collectors.toList());

        // Parent facet
        FacetDTO parent = new FacetDTO();
        parent.name = parentName;
        parent.display = parentDisplay;
        parent.description = parentDescription;
        parent.expressions = List.of(p0, p1); // parent scope remains AND
        parent.expressionGroups = null;
        parent.facets = children;

        // Category
        FacetCategoryDTO cat = new FacetCategoryDTO();
        cat.name = categoryName;
        cat.display = categoryDisplay;
        cat.description = categoryDescription;
        cat.facets = List.of(parent);

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper();
        wrapper.facetCategory = cat;
        return wrapper;
    }

    private static List<String> split(String conceptPath) {
        List<String> out = new ArrayList<>();
        if (conceptPath == null) return out;
        String[] parts = conceptPath.split("\\\\"); // split on backslash
        for (String p : parts) if (p != null && !p.isEmpty()) out.add(p);
        return out;
    }

    private static String defaultStr(String val, String def) {
        return (val == null || val.isBlank()) ? def : val;
    }

    // Request/Response DTOs
    public static class GenerateRecoverMonthsRequest {
        public String categoryName;           // default: "Consortium_Curated_Facets"
        public String categoryDisplay;        // default: derived from name
        public String categoryDescription;    // default: description

        public String parentFacetName;        // default: "Recover Adult"
        public String parentFacetDisplay;     // default: "RECOVER Adult"
        public String parentFacetDescription; // default: description

        public String studyId;                // default: "phs003463"
        public String adultNodeRegex;         // default: "(?i)RECOVER_Adult$"

        public String pathPrefixRegex;        // optional: discovery filter only
        public Boolean clearCategoryFirst;    // optional: clear whole category
        public Boolean clearParentFacetFirst; // optional: clear just the parent subtree
        public Boolean dryRun;                // optional
    }

    public static class GenerateRecoverMonthsResponse {
        public String categoryName;
        public String parentFacetName;
        public java.util.Set<String> discoveredMonths;
        public String message;
        public Result load;             // from FacetLoaderService
        public ClearResult clear;       // from FacetLoaderService
    }
}