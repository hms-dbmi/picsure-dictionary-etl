package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Evaluates FacetExpressionDTO rules against a concept_path.
 * <p>
 * Semantics (initial):
 * - Supports Logic values: "equal" and "not" (case-insensitive).
 * - Expressions within a facet are ANDed together; all must evaluate to true
 *   for the facet to apply to the concept path.
 * - nodePosition is a zero-based index addressing the concept path segments.
 *   Negative positions are supported (e.g., -1 is last node, -2 is second to last, etc.).
 * - If a nodePosition is out of bounds or missing, the expression evaluates to false.
 * - Regex strings are compiled as-is; inline flags such as (?i) are supported by Java's Pattern.
 * </p>
 */
public final class FacetExpressionEvaluator {

    private FacetExpressionEvaluator() {}

    public static boolean facetAppliesToConceptPath(List<FacetExpressionDTO> expressions, String conceptPath) {
        if (expressions == null || expressions.isEmpty()) {
            // No expressions provided: interpret as no automatic mapping.
            return false;
        }
        for (FacetExpressionDTO expr : expressions) {
            if (!evaluate(expr, conceptPath)) {
                return false; // AND semantics
            }
        }
        return true;
    }

    public static boolean evaluate(FacetExpressionDTO expr, String conceptPath) {
        if (expr == null || expr.nodePosition == null || expr.regex == null || expr.regex.isBlank() || conceptPath == null) {
            return false;
        }
        String node = getNodeAt(conceptPath, expr.nodePosition);
        if (node == null) return false;

        boolean matches;
        try {
            Pattern p = Pattern.compile(expr.regex);
            matches = p.matcher(node).find();
        } catch (Exception e) {
            // Bad regex: treat as non-match
            return false;
        }

        String logic = expr.logic == null ? "" : expr.logic.trim().toLowerCase();
        switch (logic) {
            case "equal":
                return matches;
            case "not":
                return !matches;
            default:
                // Unsupported logic -> do not apply
                return false;
        }
    }

    /**
     * Returns the node at the given position within the concept path, or null if out of bounds.
     */
    static String getNodeAt(String conceptPath, int nodePosition) {
        List<String> nodes = splitConceptPath(conceptPath);
        if (nodes.isEmpty()) return null;
        int idx = nodePosition >= 0 ? nodePosition : nodes.size() + nodePosition; // negative index support
        if (idx < 0 || idx >= nodes.size()) return null;
        return nodes.get(idx);
    }

    /**
     * Splits a HPDS-style concept path (e.g., "\\dataset\\group\\var\\") into its node segments.
     */
    static List<String> splitConceptPath(String conceptPath) {
        List<String> nodes = new ArrayList<>();
        if (conceptPath == null) return nodes;
        String[] parts = conceptPath.split("\\\\"); // split on backslash
        for (String part : parts) {
            if (part != null && !part.isEmpty()) {
                nodes.add(part);
            }
        }
        return nodes;
    }
}
