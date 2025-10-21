package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import java.util.regex.Pattern;

public final class CompiledExpr {
    final FacetExpressionDTO src;
    final Pattern regex;
    CompiledExpr(FacetExpressionDTO src) {
        this.src = src;
        this.regex = (src.regex != null && !src.regex.isBlank()) ? Pattern.compile(src.regex) : null;
    }
}
