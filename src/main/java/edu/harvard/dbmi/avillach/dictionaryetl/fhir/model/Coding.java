package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * FHIR R4 Coding datatype (subset).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Coding(String system, String code, String display) {
}
