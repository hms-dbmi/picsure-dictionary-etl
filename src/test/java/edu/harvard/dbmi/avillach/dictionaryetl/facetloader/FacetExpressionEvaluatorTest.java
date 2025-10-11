package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

class FacetExpressionEvaluatorTest {

    @Test
    void equal_shouldMatchSpecificNode_caseInsensitive() {
        String path = "\\phs003436\\Recover_Adult\\biostats_derived\\visits\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.logic = "equal";
        expr.regex = "(?i)Recover_Adult$"; // should match 2nd node (index 1)
        expr.nodePosition = 1;

        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void not_shouldFailWhenPatternFound() {
        String path = "\\A\\B\\C\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.logic = "not";
        expr.regex = "B";
        expr.nodePosition = 1;

        assertFalse(FacetExpressionEvaluator.evaluate(expr, path));
        assertFalse(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void negativeIndex_shouldAddressFromEnd() {
        String path = "\\root\\mid\\leaf\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.logic = "equal";
        expr.regex = "(?i)leaf";
        expr.nodePosition = -1; // last node

        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
    }

    @Test
    void outOfBoundsNode_returnsFalse() {
        String path = "\\only\\one\\";
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.logic = "equal";
        expr.regex = ".*";
        expr.nodePosition = 5; // oob

        assertFalse(FacetExpressionEvaluator.evaluate(expr, path));
    }
}
