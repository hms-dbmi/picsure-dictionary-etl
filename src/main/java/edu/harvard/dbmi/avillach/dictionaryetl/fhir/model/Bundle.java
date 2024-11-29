package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * Represents a FHIR Bundle resource.
 * Bundles are containers for collections of resources.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Bundle(
    String resourceType,
    String id,
    Meta meta,
    String type,
    List<Link> link,
    List<Entry> entry
) {}


