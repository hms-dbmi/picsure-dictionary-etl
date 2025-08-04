package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.Pair;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ConceptCSVIngestWrapper {

    private static final Logger log = LoggerFactory.getLogger(ConceptCSVIngestWrapper.class);
    private final CSVReader reader;
    private final long datasetId;
    private int badRows = 0, totalRows = 0 , concepts = 0, metas = 0, facetCount = 0;
    private boolean headersValid = true;
    private boolean readerHappy = true;
    private boolean rowsRemaining = true;
    private final List<String> headers;
    private final List<String> metaHeaders;
    private final Map<String, Integer> headerMap;
    private static final List<String> REQUIRED_HEADERS =
        List.of("dataset_ref", "name", "display", "concept_type", "concept_path", "parent_concept_path");
    private final boolean includeCategoryFacet, includeConceptTypeFacet;
    private final List<String> metaFacets;

    public ConceptCSVIngestWrapper(String input, long datasetId, boolean includeCategoryFacet, boolean includeConceptTypeFacet, List<String> metaFacets) {
        this.reader = new CSVReader(new StringReader(input));
        this.datasetId = datasetId;
        this.includeCategoryFacet = includeCategoryFacet;
        this.includeConceptTypeFacet = includeConceptTypeFacet;
        this.metaFacets = metaFacets;
        this.headers = validateAndSetHeaders(reader);
        this.metaHeaders = headers.stream()
            .filter(Predicate.not(REQUIRED_HEADERS::contains))
            .toList();
        this.headerMap = IntStream.range(0, headers.size()).boxed()
            .collect(Collectors.toMap(headers::get, Function.identity()));
    }

    private List<String> validateAndSetHeaders(CSVReader reader) {
        try {
            List<String> actualHeaders = List.of(reader.readNext());
            headersValid = new HashSet<>(actualHeaders).containsAll(REQUIRED_HEADERS);
            return actualHeaders;

        } catch (IOException | CsvValidationException e) {
            log.warn("Error parsing header for CSV: ", e);
            headersValid = false;
            return List.of();
        }
    }

    public Optional<ParsedCSVConceptRow> next() {
        try {
            String[] row = reader.readNext();
            if (row == null) { // EOF
                log.info("Done reading from CSV");
                rowsRemaining = false;
                return Optional.empty();
            } else {
                return Optional.ofNullable(process(row));
            }
        } catch (IOException e) {
            log.warn("Error reading from CSV reader: ", e);
            readerHappy = false;
            return Optional.empty();
        } catch (CsvValidationException e) {
            log.info("Error validating CSV row. Skipping");
            badRows++;
            return Optional.empty();
        }
    }

    private ParsedCSVConceptRow process(String[] row) {
        String conceptType = row[headerMap.get("concept_type")];
        String conceptPath = row[headerMap.get("concept_path")];
        String name = row[headerMap.get("name")];
        String display = row[headerMap.get("display")];
        String parent = row[headerMap.get("parent_concept_path")];
        ConceptModel concept = new ConceptModel();
        concept.setConceptType(conceptType);
        concept.setConceptPath(conceptPath);
        concept.setName(name);
        concept.setDisplay(display);
        concept.setDatasetId(datasetId);
        List<Pair<String, String>> pairs = metaHeaders.stream()
            .map(header -> Pair.of(
                header,
                row[headerMap.get(header)]
            ))
            .filter(pair -> StringUtils.hasLength(pair.getSecond()))
            .toList();
        List<FacetsAndPairs> facets = Stream.concat(
            Stream.of(extractCategoryFacet(conceptPath), extractDataTypeFacet(conceptPath, row)),
            extractMetadataFacets(conceptPath, row).stream()
        ).filter(Objects::nonNull).toList();
        facetCount = Math.max(facets.size(), (int) facets.stream().mapToLong(f -> f.facets().size()).sum());
        totalRows++; concepts++; metas+=pairs.size();
        return new ParsedCSVConceptRow(concept, pairs, parent, facets);
    }

    private FacetsAndPairs extractCategoryFacet(String conceptPath) {
        if(!includeCategoryFacet) {
            return null;
        }
        List<String> nodes = Arrays.stream(conceptPath.split("\\\\")).filter(StringUtils::hasLength).toList();
        if (nodes.size() >= 4) {
            // create nested facet, assign node to child
            String parent = nodes.get(0).replaceAll("([\\[\\]])", "");
            String child = nodes.get(1).replaceAll("([\\[\\]])", "");
            return new FacetsAndPairs(
                "category",
                List.of(
                    // append category to name to guarantee uniqueness
                    new NameDisplayCategory("category_" + parent.toLowerCase().replaceAll(" ", "_"), parent, "category"),
                    new NameDisplayCategory("category_" + child.toLowerCase().replaceAll(" ", "_"), child, "category")
                ),
                conceptPath
            );
        }
        if (nodes.size() == 3) {
            // create non nested facet
            String parent = nodes.getFirst().replaceAll("([\\[\\]])", "");
            return new FacetsAndPairs(
                "category",
                List.of(new NameDisplayCategory("category_" + parent.toLowerCase().replaceAll(" ", "_"), parent, "category")),
                conceptPath
            );
        }
        return null;
    }

    private FacetsAndPairs extractDataTypeFacet(String conceptPath, String[] row) {
        if (!includeCategoryFacet) {
            return null;
        }
        String conceptType = row[headerMap.get("concept_type")];
        return new FacetsAndPairs(
            "data_type",
            List.of(new NameDisplayCategory("data_type_" + conceptType.toLowerCase().replaceAll(" ", "_"), conceptType, "data_type")),
            conceptPath
        );
    }

    private List<FacetsAndPairs> extractMetadataFacets(String conceptPath, String[] row) {
        return metaFacets.stream()
            .filter(metaHeaders::contains)
            .filter(key -> StringUtils.hasLength(row[headerMap.get(key)]))
            .map(key -> {
                String value = row[headerMap.get(key)];
                String category = key.toLowerCase().replaceAll(" ", "_");
                return new FacetsAndPairs(
                    category,
                    List.of(new NameDisplayCategory(category + "_" + value.toLowerCase().replaceAll(" ", "_"), value, "category")),
                    conceptPath
                );
            })
            .toList();
    }

    public boolean shouldContinue() {
        return headersValid && readerHappy && rowsRemaining;
    }

    public ConceptCSVManifest createManifest() {
        return new ConceptCSVManifest(
            totalRows, badRows, concepts, metas, facetCount,
            !shouldContinue(), headersValid, readerHappy
        );
    }
}
