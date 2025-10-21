package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

class FacetExpressionEvaluatorTest {

    @Test
    void regex_shouldMatchSpecificNode_caseInsensitive() {
        String path = "\\phs003436\\Recover_Adult\\biostats_derived\\visits\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.regex = "(?i)Recover_Adult$"; // should match 2nd node (index 1)
        expr.node = 1;

        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void contains_shouldMatchSubstring() {
        String path = "\\A\\BXYZ\\C\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.contains = "B";
        expr.node = 1;

        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void negativeIndex_shouldAddressFromEnd() {
        String path = "\\root\\mid\\leaf\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.regex = "(?i)leaf";
        expr.node = -1; // last node

        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
    }

    @Test
    void outOfBoundsNode_returnsFalse() {
        String path = "\\only\\one\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.regex = ".*";
        expr.node = 5; // oob

        assertFalse(FacetExpressionEvaluator.evaluate(expr, path));
    }

    @Test
    void omitNode_shouldScanAllNodes_andMatchContains() {
        String path = "\\A\\BXYZ\\C\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.contains = "B"; // should match node 1 when scanning all
        // expr.node omitted (null)
        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void omitNode_shouldScanAllNodes_andMatchExactlyLast() {
        String path = "\\root\\mid\\leaf\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.exactly = "leaf";
        // node omitted, should still match
        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
    }

    @Test
    void mixedEntries_AND_semantics_withOmittedNode() {
        String path = "\\A\\B\\C\\";
        FacetExpressionDTO anyC = new FacetExpressionDTO();
        anyC.exactly = "C"; // matches last when scanning all nodes
        FacetExpressionDTO specificB = new FacetExpressionDTO();
        specificB.exactly = "B";
        specificB.node = 1;
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(anyC, specificB), path));
    }

    @Test
    void omitNode_butNoMatchers_returnsFalse() {
        String path = "\\A\\B\\C\\";
        FacetExpressionDTO empty = new FacetExpressionDTO();
        assertFalse(FacetExpressionEvaluator.evaluate(empty, path));
    }
}
