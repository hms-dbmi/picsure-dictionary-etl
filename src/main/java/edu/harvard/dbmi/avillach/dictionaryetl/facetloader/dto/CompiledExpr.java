package edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto;

import java.util.regex.Pattern;

public final class CompiledExpr {
    public final FacetExpressionDTO src;
    public final Pattern regex;
    public CompiledExpr(FacetExpressionDTO src) {
        this.src = src;
        this.regex = (src.regex != null && !src.regex.isBlank()) ? Pattern.compile(src.regex) : null;
    }
}
