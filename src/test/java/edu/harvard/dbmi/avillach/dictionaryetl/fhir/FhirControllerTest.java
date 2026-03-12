package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ContextConfiguration(classes = {
    FhirController.class
})
public class FhirControllerTest {

    @Autowired
    private FhirController fhirController;

    @MockitoBean
    private FhirService fhirService;

    @Test
    void testDatasetsMetadataRefresh_Success() throws IOException {
        doNothing().when(fhirService).updateDatasetMetadata();

        ResponseEntity<String> response = fhirController.datasetsMetadataRefresh();

        assertEquals(200, response.getStatusCodeValue());
        assertEquals("Metadata update successful", response.getBody());
        verify(fhirService, times(1)).updateDatasetMetadata();
    }

    @Test
    void testDatasetsMetadataRefresh_IOExceptionThrown() throws IOException {
        doThrow(new IOException("FHIR API connection failed"))
            .when(fhirService).updateDatasetMetadata();

        ResponseEntity<String> response = fhirController.datasetsMetadataRefresh();

        assertEquals(500, response.getStatusCodeValue());
        assertEquals("Metadata update failed", response.getBody());
        verify(fhirService, times(1)).updateDatasetMetadata();
    }

    @Test
    void testUpdateUrlToKeyMap_Success() {
        String urlToKeyMapJson = """
            {
              "dbgap_study_accession": "Study Accession",
              "participant_set": "Participant Set"
            }
            """;

        doNothing().when(fhirService).setUrlToKeyMap(anyString());

        ResponseEntity<String> response = fhirController.updateUrlToKeyMap(urlToKeyMapJson);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals("URL to Key Map updated successfully", response.getBody());
        verify(fhirService, times(1)).setUrlToKeyMap(urlToKeyMapJson);
    }

    @Test
    void testUpdateUrlToKeyMap_EmptyJson() {
        String emptyJson = "";

        doNothing().when(fhirService).setUrlToKeyMap(anyString());

        ResponseEntity<String> response = fhirController.updateUrlToKeyMap(emptyJson);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals("URL to Key Map updated successfully", response.getBody());
        verify(fhirService, times(1)).setUrlToKeyMap(emptyJson);
    }

    @Test
    void testFindAll_Success() throws IOException {
        ResearchStudy study1 = new ResearchStudy(
            "ResearchStudy",
            "phs000001.v1.p1",
            null,
            "Test Study 1",
            "Test Study 1 Description",
            null
        );
        ResearchStudy study2 = new ResearchStudy(
            "ResearchStudy",
            "phs000002.v1.p1",
            null,
            "Test Study 2",
            "Test Study 2 Description",
            null
        );
        List<ResearchStudy> mockStudies = Arrays.asList(study1, study2);

        when(fhirService.getResearchStudies()).thenReturn(mockStudies);

        ResponseEntity<List<ResearchStudy>> response = fhirController.findAll();

        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertEquals(2, response.getBody().size());
        assertEquals("phs000001.v1.p1", response.getBody().get(0).id());
        verify(fhirService, times(1)).getResearchStudies();
    }

    @Test
    void testFindAll_EmptyList() throws IOException {
        when(fhirService.getResearchStudies()).thenReturn(Collections.emptyList());

        ResponseEntity<List<ResearchStudy>> response = fhirController.findAll();

        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty());
        verify(fhirService, times(1)).getResearchStudies();
    }

    @Test
    void testFindAll_IOExceptionThrown() throws IOException {
        when(fhirService.getResearchStudies())
            .thenThrow(new IOException("Failed to fetch research studies"));

        ResponseEntity<List<ResearchStudy>> response = fhirController.findAll();

        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
        verify(fhirService, times(1)).getResearchStudies();
    }

    @Test
    void testGetDistinctPhsValues_Success() throws IOException {
        List<String> mockPhsValues = Arrays.asList("phs000001", "phs000002", "phs000003");

        when(fhirService.getDistinctPhsValues()).thenReturn(mockPhsValues);

        ResponseEntity<List<String>> response = fhirController.getDistinctPhsValues();

        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertEquals(3, response.getBody().size());
        assertTrue(response.getBody().contains("phs000001"));
        verify(fhirService, times(1)).getDistinctPhsValues();
    }

    @Test
    void testGetDistinctPhsValues_EmptyList() throws IOException {
        when(fhirService.getDistinctPhsValues()).thenReturn(Collections.emptyList());

        ResponseEntity<List<String>> response = fhirController.getDistinctPhsValues();

        assertEquals(200, response.getStatusCodeValue());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().isEmpty());
        verify(fhirService, times(1)).getDistinctPhsValues();
    }

    @Test
    void testGetDistinctPhsValues_IOExceptionThrown() throws IOException {
        when(fhirService.getDistinctPhsValues())
            .thenThrow(new IOException("Failed to extract PHS values"));

        ResponseEntity<List<String>> response = fhirController.getDistinctPhsValues();

        assertEquals(500, response.getStatusCodeValue());
        assertNull(response.getBody());
        verify(fhirService, times(1)).getDistinctPhsValues();
    }

    @Test
    void testUpdateUrlToKeyMap_NullJson() {
        doNothing().when(fhirService).setUrlToKeyMap(null);

        ResponseEntity<String> response = fhirController.updateUrlToKeyMap(null);

        assertEquals(200, response.getStatusCodeValue());
        assertEquals("URL to Key Map updated successfully", response.getBody());
        verify(fhirService, times(1)).setUrlToKeyMap(null);
    }
}
