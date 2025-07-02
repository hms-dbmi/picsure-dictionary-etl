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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConceptCSVIngestWrapper {

    private static final Logger log = LoggerFactory.getLogger(ConceptCSVIngestWrapper.class);
    private final CSVReader reader;
    private final long datasetId;
    private int badRows = 0, totalRows = 0 , concepts = 0, metas = 0;
    private boolean headersValid = true;
    private boolean readerHappy = true;
    private boolean rowsRemaining = true;
    private final List<String> headers;
    private final List<String> metaHeaders;
    private final Map<String, Integer> headerMap;
    private static final List<String> REQUIRED_HEADERS =
        List.of("dataset_ref", "name", "display", "concept_type", "concept_path", "parent_concept_path");

    public ConceptCSVIngestWrapper(String input, long datasetId) {
        this.reader = new CSVReader(new StringReader(input));
        this.datasetId = datasetId;
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

    public Optional<ConceptAndMetas> next() {
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

    private ConceptAndMetas process(String[] row) {
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
        totalRows++; concepts++; metas+=pairs.size();
        return new ConceptAndMetas(concept, pairs, parent);
    }

    public boolean shouldContinue() {
        return headersValid && readerHappy && rowsRemaining;
    }

    public ConceptCSVManifest createManifest() {
        return new ConceptCSVManifest(
            totalRows, badRows, concepts, metas,
            !shouldContinue(), headersValid, readerHappy
        );
    }
}
