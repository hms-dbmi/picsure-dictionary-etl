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
}
