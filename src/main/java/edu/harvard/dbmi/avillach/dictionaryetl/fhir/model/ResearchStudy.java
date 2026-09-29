package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * FHIR R4 ResearchStudy resource (subset). {@code category}, {@code sponsor} and {@code focus} are the standard fields that replace the
 * legacy {@code DBGAP-FHIR-*} extensions.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ResearchStudy(
    String resourceType, String id, Meta meta, String title, String description, List<Extension> extension, List<CodeableConcept> category,
    Reference sponsor, List<CodeableConcept> focus
) {
}
