package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Evaluates FacetExpressionDTO rules against a concept_path using the simplified
 * expression format: each entry may specify one or more of {exactly, contains, regex}
 * and must include a node index. All entries are ANDed together.
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
        if (expr == null || expr.node == null || conceptPath == null) {
            return false;
        }
        String nodeVal = getNodeAt(conceptPath, expr.node);
        if (nodeVal == null) return false;

        boolean ok = true;

        // exactly match
        if (expr.exactly != null && !expr.exactly.isBlank()) {
            ok &= nodeVal.equals(expr.exactly);
            if (!ok) return false;
        }
        // contains substring
        if (expr.contains != null && !expr.contains.isBlank()) {
            ok &= nodeVal.contains(expr.contains);
            if (!ok) return false;
        }
        // regex match
        if (expr.regex != null && !expr.regex.isBlank()) {
            try {
                Pattern p = Pattern.compile(expr.regex);
                ok &= p.matcher(nodeVal).find();
                if (!ok) return false;
            } catch (Exception e) {
                return false; // bad regex
            }
        }

        // If none of the matchers were provided, treat as non-match
        if ((expr.exactly == null || expr.exactly.isBlank()) &&
            (expr.contains == null || expr.contains.isBlank()) &&
            (expr.regex == null || expr.regex.isBlank())) {
            return false;
        }

        return ok;
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
