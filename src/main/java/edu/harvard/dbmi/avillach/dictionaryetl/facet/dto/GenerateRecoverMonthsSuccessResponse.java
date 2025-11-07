package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import java.util.Set;

public record GenerateRecoverMonthsSuccessResponse(
        String message,
        String categoryName,
        String parentFacetName,
        Set<String> discoveredMonths,
        Result load,
        ClearResult clear
) implements GenerateRecoverMonthsResponse {}
