package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Extension;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Meta;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.util.LinkedMultiValueMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Tag("unit")
@WebMvcTest(FhirController.class)
public class FhirControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private FhirService fhirService;

    private List<ResearchStudy> mockResearchStudies;
    private List<String> mockDistinctPhsValues;

    @BeforeEach
    public void setup() throws IOException {
        Meta mockMeta = new Meta("test");
        Extension mockExtension = new Extension("url", String.valueOf(new LinkedMultiValueMap<>()));

        ResearchStudy study1 = new ResearchStudy("ResearchStudy", "id1", mockMeta, "title1", "description1", Arrays.asList(mockExtension));
        ResearchStudy study2 = new ResearchStudy("ResearchStudy", "id2", mockMeta, "title2", "description2", Arrays.asList(mockExtension));

        mockResearchStudies = Arrays.asList(study1, study2);
        mockDistinctPhsValues = Arrays.asList("phs000007", "phs000200");

        when(fhirService.getResearchStudies()).thenReturn(mockResearchStudies);
        when(fhirService.getDistinctPhsValues()).thenReturn(mockDistinctPhsValues);
    }

    @Test
    public void testDatasetsMetadataRefreshSuccess() throws Exception {
        mockMvc.perform(get("/api/fhir/datasets/metadata/update"))
                .andExpect(status().isOk())
                .andExpect(content().string("Metadata update successful"));
    }

    @Test
    public void testDatasetsMetadataRefreshFailure() throws Exception {
        doThrow(new IOException("IO Error")).when(fhirService).updateDatasetMetadata();

        mockMvc.perform(get("/api/fhir/datasets/metadata/update"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().string("Metadata update failed"));
    }

    @Test
    public void testGetResearchStudiesSuccess() throws Exception {
        mockMvc.perform(get("/api/fhir/research-studies"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().json("[{}, {}]")); // Adjust expected JSON based on test data
    }

    @Test
    public void testGetResearchStudiesFailure() throws Exception {
        doThrow(new IOException("IO Error")).when(fhirService).getResearchStudies();

        mockMvc.perform(get("/api/fhir/research-studies"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    public void testGetDistinctPhsValuesSuccess() throws Exception {
        mockMvc.perform(get("/api/fhir/distinct-phs-values"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().json("[\"phs000007\", \"phs000200\"]"));
    }

    @Test
    public void testGetDistinctPhsValuesFailure() throws Exception {
        doThrow(new IOException("IO Error")).when(fhirService).getDistinctPhsValues();

        mockMvc.perform(get("/api/fhir/distinct-phs-values"))
                .andExpect(status().isInternalServerError());
    }
}