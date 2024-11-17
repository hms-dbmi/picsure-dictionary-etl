
package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

import java.util.List;

public record ResearchStudy(
    String resourceType,
    String id,
    Meta meta,
    String title,
    String description,
    List<Extension> extension
) {}
