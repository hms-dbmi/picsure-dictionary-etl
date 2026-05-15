package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

/**
 * Carries the data needed to map a single metadata-driven leaf facet.
 * The facet collects every concept whose metadata entry for {@code metaKey}
 * equals {@code metaValue}.
 */
public record MetaLeafFacetSpec(Long facetId, String metaKey, String metaValue) {}
