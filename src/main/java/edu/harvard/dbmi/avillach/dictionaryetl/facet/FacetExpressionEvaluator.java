package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetExpressionDTO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Evaluates FacetExpressionDTO rules against a concept_path using the simplified
 * expression format: each entry may specify one or more of {exactly, contains, regex}.
 * Node is optional: if provided, we evaluate only that node; if omitted, we scan all nodes
 * and return true if any node satisfies the entry. All entries are ANDed together.
 */
public final class FacetExpressionEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(FacetExpressionEvaluator.class);

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
        for (List<FacetExpressionDTO> group : groups) {
            if (facetAppliesToConceptPath(group, conceptPath)) {
                return true; // OR semantics
            }
        }
        return false;
    }

    public static boolean evaluate(FacetExpressionDTO expr, String conceptPath) {
        List<String> nodes = splitConceptPath(conceptPath);

        // If no matcher fields are provided, treat as non-match
        if (isEmpty(expr)) {
            return false;
        }

        // If node index provided, evaluate only that node
        if (expr.node() != null) {
            String nodeVal = getNodeAt(nodes, expr.node());
            if (nodeVal == null) {
                return false;
            }

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

    private static boolean matchesNode(FacetExpressionDTO expr, String nodeVal) {
        if (StringUtils.isNotBlank(expr.exactly())) {
            return nodeVal.equals(expr.exactly());
        }

        if (StringUtils.isNotBlank(expr.contains())) {
            return nodeVal.contains(expr.contains());
        }

        if (StringUtils.isNotBlank(expr.regex())) {
            try {
                Pattern p = Pattern.compile(expr.regex());
                return p.matcher(nodeVal).find();
            } catch (Exception e) {
                logger.error("matchesNode() - Expression {} - regex could not be compiled: {}", expr, expr.regex());
                return false;
            }
        }

        return false;
    }

    private static boolean isEmpty(FacetExpressionDTO expr) {
        return (StringUtils.isBlank(expr.exactly()))
               && (StringUtils.isBlank(expr.contains()))
               && (StringUtils.isBlank(expr.regex()));
    }

    private static String getNodeAt(List<String> nodes, int nodePosition) {
        int idx = nodePosition >= 0 ? nodePosition : nodes.size() + nodePosition; // negative index support
        if (idx < 0 || idx >= nodes.size()) { // Index is out of bounds.
            return null;
        }
        return nodes.get(idx);
    }

    /**
     * Splits a HPDS-style concept path (e.g., "\\dataset\\group\\var\\") into its node segments.
     */
    static List<String> splitConceptPath(String conceptPath) {
        return Arrays.stream(conceptPath.split("\\\\")).filter(StringUtils::isNotBlank).toList();
    }

}