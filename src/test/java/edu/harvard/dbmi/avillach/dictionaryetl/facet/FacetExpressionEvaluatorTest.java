package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetExpressionDTO;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

class FacetExpressionEvaluatorTest {

    @Test
    void regex_shouldMatchSpecificNode_caseInsensitive() {
        String path = "\\phs003463\\Recover_Adult\\biostats_derived\\visits\\";
        FacetExpressionDTO expr = new FacetExpressionDTO(null, null, "(?i)Recover_Adult$", 1);
        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void contains_shouldMatchSubstring() {
        String path = "\\A\\BXYZ\\C\\";
        FacetExpressionDTO expr = new FacetExpressionDTO(null, "B", null, 1);
        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void negativeIndex_shouldAddressFromEnd() {
        String path = "\\root\\mid\\leaf\\";
        FacetExpressionDTO expr = new FacetExpressionDTO(null, null, "(?i)leaf", -1);
        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
    }

    @Test
    void outOfBoundsNode_returnsFalse() {
        String path = "\\only\\one\\";
        FacetExpressionDTO expr = new FacetExpressionDTO(null, null, ".*", 5);
        assertFalse(FacetExpressionEvaluator.evaluate(expr, path));
    }

    @Test
    void omitNode_shouldScanAllNodes_andMatchContains() {
        String path = "\\A\\BXYZ\\C\\";
        FacetExpressionDTO expr = new FacetExpressionDTO(null, "B", null, null); // scan all nodes
        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(expr), path));
    }

    @Test
    void omitNode_shouldScanAllNodes_andMatchExactlyLast() {
        String path = "\\root\\mid\\leaf\\";
        FacetExpressionDTO expr = new FacetExpressionDTO("leaf", null, null, null);
        assertTrue(FacetExpressionEvaluator.evaluate(expr, path));
    }

    @Test
    void mixedEntries_AND_semantics_withOmittedNode() {
        String path = "\\A\\B\\C\\";
        FacetExpressionDTO anyC = new FacetExpressionDTO("C", null, null, null); // matches last when scanning all nodes
        FacetExpressionDTO specificB = new FacetExpressionDTO("B", null, null, 1);
        assertTrue(FacetExpressionEvaluator.facetAppliesToConceptPath(List.of(anyC, specificB), path));
    }

    @Test
    void omitNode_butNoMatchers_returnsFalse() {
        String path = "\\A\\B\\C\\";
        FacetExpressionDTO empty = new FacetExpressionDTO(null, null, null, null);
        assertFalse(FacetExpressionEvaluator.evaluate(empty, path));
    }
}
