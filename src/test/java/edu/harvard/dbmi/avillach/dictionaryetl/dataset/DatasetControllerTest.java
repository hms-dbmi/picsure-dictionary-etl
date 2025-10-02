package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatasetControllerTest {

    private DatasetController controller;

    @Mock
    private DatasetService datasetService;

    @Mock
    private FacetService facetService;

    @BeforeEach
    void setUp() {
        controller = new DatasetController();
        ReflectionTestUtils.setField(controller, "datasetService", datasetService);
        ReflectionTestUtils.setField(controller, "facetService", facetService);
    }

    @Test
    void deleteDataset_whenNoneDeleted_returnsNoContent_andCallsFacetService() {
        String ref = "REF_0";
        when(datasetService.deleteByRef(ref)).thenReturn(0);

        ResponseEntity<String> response = controller.deleteDataset(ref);

        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        assertEquals("No dataset found to delete", response.getBody());
        verify(datasetService, times(1)).deleteByRef(ref);
        verify(facetService, times(1)).deleteByName(ref);
        verifyNoMoreInteractions(datasetService, facetService);
    }

    @Test
    void deleteDataset_whenDeleted_returnsOk_andCallsFacetService() {
        String ref = "REF_1";
        when(datasetService.deleteByRef(ref)).thenReturn(1);

        ResponseEntity<String> response = controller.deleteDataset(ref);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Dataset deleted", response.getBody());
        verify(datasetService, times(1)).deleteByRef(ref);
        verify(facetService, times(1)).deleteByName(ref);
        verifyNoMoreInteractions(datasetService, facetService);
    }
}
