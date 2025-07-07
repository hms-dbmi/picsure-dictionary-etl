package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

public record ConceptCSVManifest(
    int totalRows, int badRows, int concepts, int metas,
    int facets, boolean complete, boolean headersValid, boolean csvValid
) {
}
