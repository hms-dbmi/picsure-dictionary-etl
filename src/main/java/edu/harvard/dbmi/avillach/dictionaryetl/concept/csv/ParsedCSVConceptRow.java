package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import org.springframework.data.util.Pair;

import java.util.List;

public record ParsedCSVConceptRow(
    ConceptModel concept, List<Pair<String, String>> metas, String parentPath,
    List<FacetsAndPairs> facetsAndPairs) {
}
