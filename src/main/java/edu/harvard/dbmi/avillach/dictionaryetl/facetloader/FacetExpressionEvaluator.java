package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.CompiledExpr;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.FacetExpressionDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Evaluates FacetExpressionDTO rules against a concept_path using the simplified
 * expression format: each entry may specify one or more of {exactly, contains, regex}.
 * Node is optional: if provided, we evaluate only that node; if omitted, we scan all nodes
 * and return true if any node satisfies the entry. All entries are ANDed together.
 *
 * Added support for grouped OR semantics via {@link #facetAppliesToConceptPathGrouped(List, String)}.
 */
public final class FacetExpressionEvaluator {

    private FacetExpressionEvaluator() {
    }

    /** Legacy: AND across all expressions */
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

    /** New: OR across groups, AND within each group */
    public static boolean facetAppliesToConceptPathGrouped(List<List<FacetExpressionDTO>> groups, String conceptPath) {
        if (groups == null || groups.isEmpty()) {
            return false;
        }
        for (List<FacetExpressionDTO> group : groups) {
            if (facetAppliesToConceptPath(group, conceptPath)) {
                return true; // OR semantics
            }
        }
        return false;
    }

    public static boolean evaluate(FacetExpressionDTO expr, String conceptPath) {
        if (expr == null || conceptPath == null) {
            return false;
        }
        List<String> nodes = splitConceptPath(conceptPath);
        if (nodes.isEmpty()) return false;

        // If no matcher fields are provided, treat as non-match
        if (isEmpty(expr)) return false;

        // If node index provided, evaluate only that node
        if (expr.node != null) {
            String nodeVal = getNodeAt(nodes, expr.node);
            if (nodeVal == null) return false;
            return matchesNode(expr, nodeVal);
        }

        // No node provided: evaluate against ALL nodes; true if any matches
        for (String nodeVal : nodes) {
            if (matchesNode(expr, nodeVal)) {
                return true;
            }
        }
        return false;
    }

    /** Legacy compiled-path (AND). Kept for compatibility with existing callers. */
    public static boolean applies(List<CompiledExpr> compiled, String conceptPath) {
        if (compiled == null || compiled.isEmpty()) return false;
        for (CompiledExpr ce : compiled) {
            FacetExpressionDTO e = ce.src;
            if (!evaluateWithCache(e, ce.regex, conceptPath)) return false;
        }
        return true;
    }

    private static boolean evaluateWithCache(FacetExpressionDTO expr, java.util.regex.Pattern cachedRegex, String conceptPath) {
        // Mirror FacetExpressionEvaluator.evaluate semantics, but reuse regex
        if (expr == null || conceptPath == null) return false;
        java.util.List<String> nodes = FacetExpressionEvaluator.splitConceptPath(conceptPath);
        if (nodes.isEmpty()) return false;
        boolean hasMatcher = !isBlank(expr.exactly) || !isBlank(expr.contains) || !isBlank(expr.regex);
        if (!hasMatcher) return false;

        java.util.function.Predicate<String> matchesNode = nodeVal -> {
            boolean ok = true;
            if (!isBlank(expr.exactly)) {
                ok &= nodeVal.equals(expr.exactly);
                if (!ok) return false;
            }
            if (!isBlank(expr.contains)) {
                ok &= nodeVal.contains(expr.contains);
                if (!ok) return false;
            }
            if (!isBlank(expr.regex)) {
                try {
                    java.util.regex.Pattern p = (cachedRegex != null) ? cachedRegex : java.util.regex.Pattern.compile(expr.regex);
                    ok &= p.matcher(nodeVal).find();
                    if (!ok) return false;
                } catch (Exception ignoreBadRegex) {
                    return false;
                }
            }
            return ok;
        };

        if (expr.node != null) {
            String nodeVal = getNodeAt(nodes, expr.node);
            return nodeVal != null && matchesNode.test(nodeVal);
        }

        for (String nodeVal : nodes) {
            if (matchesNode.test(nodeVal)) return true;
        }
        return false;
    }

    private static boolean matchesNode(FacetExpressionDTO expr, String nodeVal) {
        boolean ok = true;
        if (expr.exactly != null && !expr.exactly.isBlank()) {
            ok &= nodeVal.equals(expr.exactly);
            if (!ok) return false;
        }
        if (expr.contains != null && !expr.contains.isBlank()) {
            ok &= nodeVal.contains(expr.contains);
            if (!ok) return false;
        }
        if (expr.regex != null && !expr.regex.isBlank()) {
            try {
                Pattern p = Pattern.compile(expr.regex);
                ok &= p.matcher(nodeVal).find();
                if (!ok) return false;
            } catch (Exception e) {
                return false; // bad regex
            }
        }
        return ok;
    }

    private static boolean isEmpty(FacetExpressionDTO expr) {
        return (expr.exactly == null || expr.exactly.isBlank())
               && (expr.contains == null || expr.contains.isBlank())
               && (expr.regex == null || expr.regex.isBlank());
    }

    private static String getNodeAt(List<String> nodes, int nodePosition) {
        if (nodes == null || nodes.isEmpty()) return null;
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

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}