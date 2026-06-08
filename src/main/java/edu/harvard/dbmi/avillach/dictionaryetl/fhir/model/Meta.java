package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents FHIR Meta resource metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Meta(
    String versionId,
    String lastUpdated,
    String source
) {}
