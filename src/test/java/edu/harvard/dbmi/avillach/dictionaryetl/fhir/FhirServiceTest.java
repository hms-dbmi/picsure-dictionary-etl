package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FhirServiceTest {

    @Mock
    private WebClient.Builder webClientBuilder;

    @Mock
    private WebClient webClient;

    @Mock
    private WebClient.RequestHeadersUriSpec requestHeadersUriSpec;

    @Mock
    private WebClient.RequestHeadersSpec requestHeadersSpec;

    @Mock
    private WebClient.ResponseSpec responseSpec;

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private DatasetMetadataRepository datasetMetadataRepository;

    private ObjectMapper objectMapper;
    private FhirService fhirService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        lenient().when(webClientBuilder.baseUrl(anyString())).thenReturn(webClientBuilder);
        lenient().when(webClientBuilder.exchangeStrategies(any(ExchangeStrategies.class))).thenReturn(webClientBuilder);
        lenient().when(webClientBuilder.build()).thenReturn(webClient);
        lenient().when(webClient.get()).thenReturn(requestHeadersUriSpec);
        lenient().when(requestHeadersUriSpec.uri(anyString())).thenReturn(requestHeadersSpec);
        lenient().when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);

        fhirService = new FhirService(
            webClientBuilder,
            objectMapper,
            datasetRepository,
            datasetMetadataRepository,
            "https://test-fhir-api.example.com",
            10 * 1024 * 1024  // 10MB buffer
        );

        ReflectionTestUtils.setField(fhirService, "fhirBulkEndpoint", "/ResearchStudy");
        ReflectionTestUtils.setField(fhirService, "fhirPageSize", 500);
    }

    @Test
    void testGetResearchStudies_SinglePage() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": "Test Study 1"
                  }
                },
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000002.v1.p1",
                    "description": "Test Study 2"
                  }
                }
              ]
            }
            """;

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));

        List<ResearchStudy> results = fhirService.getResearchStudies();

        assertNotNull(results);
        assertEquals(2, results.size());
        assertEquals("phs000001.v1.p1", results.get(0).id());
        assertEquals("Test Study 1", results.get(0).description());
        assertEquals("phs000002.v1.p1", results.get(1).id());
        assertEquals("Test Study 2", results.get(1).description());
    }

    @Test
    void testGetResearchStudies_MultiplePages() throws IOException {
        String page1Json = """
            {
              "resourceType": "Bundle",
              "link": [
                {
                  "relation": "next",
                  "url": "https://test-fhir-api.example.com/ResearchStudy?_count=500&page=2"
                }
              ],
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": "Test Study 1"
                  }
                }
              ]
            }
            """;

        String page2Json = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000002.v1.p1",
                    "description": "Test Study 2"
                  }
                }
              ]
            }
            """;

        when(responseSpec.bodyToMono(String.class))
            .thenReturn(Mono.just(page1Json))
            .thenReturn(Mono.just(page2Json));

        List<ResearchStudy> results = fhirService.getResearchStudies();

        assertNotNull(results);
        assertEquals(2, results.size());
        assertEquals("phs000001.v1.p1", results.get(0).id());
        assertEquals("phs000002.v1.p1", results.get(1).id());
        verify(webClient, times(2)).get();
    }

    @Test
    void testGetResearchStudies_EmptyBundle() throws IOException {
        String emptyBundleJson = """
            {
              "resourceType": "Bundle"
            }
            """;

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(emptyBundleJson));

        List<ResearchStudy> results = fhirService.getResearchStudies();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    void testGetDistinctPhsValues() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": "Test Study 1"
                  }
                },
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v2.p1",
                    "description": "Test Study 1 v2"
                  }
                },
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000002.v1.p1",
                    "description": "Test Study 2"
                  }
                }
              ]
            }
            """;

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));

        List<String> distinctPhsValues = fhirService.getDistinctPhsValues();

        assertNotNull(distinctPhsValues);
        assertEquals(2, distinctPhsValues.size());
        assertTrue(distinctPhsValues.contains("phs000001"));
        assertTrue(distinctPhsValues.contains("phs000002"));
    }

    @Test
    void testGetDistinctPhsValues_FiltersNonPhsIds() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": "Valid PHS Study"
                  }
                },
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "invalid-id",
                    "description": "Invalid Study"
                  }
                },
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "description": "Study with null id"
                  }
                }
              ]
            }
            """;

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));

        List<String> distinctPhsValues = fhirService.getDistinctPhsValues();

        assertNotNull(distinctPhsValues);
        assertEquals(1, distinctPhsValues.size());
        assertTrue(distinctPhsValues.contains("phs000001"));
    }

    @Test
    void testSetUrlToKeyMap_ValidJson() {
        String validJson = """
            {
              "dbgap_study_accession": "Study Accession",
              "participant_set": "Participant Set"
            }
            """;

        assertDoesNotThrow(() -> fhirService.setUrlToKeyMap(validJson));
    }

    @Test
    void testSetUrlToKeyMap_InvalidJson() {
        String invalidJson = "{ invalid json }";

        assertDoesNotThrow(() -> fhirService.setUrlToKeyMap(invalidJson));
    }

    @Test
    void testSetUrlToKeyMap_NullJson() {
        assertDoesNotThrow(() -> fhirService.setUrlToKeyMap(null));
    }

    @Test
    void testSetUrlToKeyMap_EmptyJson() {
        assertDoesNotThrow(() -> fhirService.setUrlToKeyMap(""));
    }

    @Test
    void testUpdateDatasetMetadata_DatasetExists() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": "Updated Description",
                    "extension": [
                      {
                        "url": "https://example.com/dbgap_study_accession",
                        "valueString": "phs000001"
                      }
                    ]
                  }
                }
              ]
            }
            """;

        DatasetModel existingDataset = new DatasetModel();
        existingDataset.setDatasetId(1L);
        existingDataset.setRef("phs000001");
        existingDataset.setDescription("Old Description");

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetRepository.findByRef("phs000001")).thenReturn(Optional.of(existingDataset));
        when(datasetMetadataRepository.findByDatasetIdAndKey(anyLong(), anyString())).thenReturn(Optional.empty());

        String urlToKeyMapJson = """
            {
              "dbgap_study_accession": "Study Accession"
            }
            """;
        fhirService.setUrlToKeyMap(urlToKeyMapJson);

        fhirService.updateDatasetMetadata();

        verify(datasetRepository).save(argThat(dataset ->
            "Updated Description".equals(dataset.getDescription())
        ));
        verify(datasetMetadataRepository).save(any(DatasetMetadataModel.class));
    }

    @Test
    void testUpdateDatasetMetadata_DatasetNotFound() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs999999.v1.p1",
                    "description": "Non-existent Dataset"
                  }
                }
              ]
            }
            """;

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetRepository.findByRef("phs999999")).thenReturn(Optional.empty());

        fhirService.updateDatasetMetadata();

        verify(datasetRepository, never()).save(any());
        verify(datasetMetadataRepository, never()).save(any());
    }

    @Test
    void testUpdateDatasetMetadata_UpdatesExistingMetadata() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": "Test Study",
                    "extension": [
                      {
                        "url": "https://example.com/dbgap_study_accession",
                        "valueString": "phs000001-updated"
                      }
                    ]
                  }
                }
              ]
            }
            """;

        DatasetModel existingDataset = new DatasetModel();
        existingDataset.setDatasetId(1L);
        existingDataset.setRef("phs000001");

        DatasetMetadataModel existingMetadata = new DatasetMetadataModel(1L, "Study Accession", "old-value");

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetRepository.findByRef("phs000001")).thenReturn(Optional.of(existingDataset));
        when(datasetMetadataRepository.findByDatasetIdAndKey(1L, "Study Accession"))
            .thenReturn(Optional.of(existingMetadata));

        String urlToKeyMapJson = """
            {
              "dbgap_study_accession": "Study Accession"
            }
            """;
        fhirService.setUrlToKeyMap(urlToKeyMapJson);

        fhirService.updateDatasetMetadata();

        assertEquals("phs000001-updated", existingMetadata.getValue());
        verify(datasetMetadataRepository, never()).save(any());
    }

    @Test
    void testUpdateDatasetMetadata_SkipsBlankDescription() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": ""
                  }
                }
              ]
            }
            """;

        DatasetModel existingDataset = new DatasetModel();
        existingDataset.setDatasetId(1L);
        existingDataset.setRef("phs000001");
        existingDataset.setDescription("Original Description");

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetRepository.findByRef("phs000001")).thenReturn(Optional.of(existingDataset));

        fhirService.updateDatasetMetadata();

        assertEquals("Original Description", existingDataset.getDescription());
    }

    @Test
    void testUpdateDatasetMetadata_HandlesNullExtensions() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000001.v1.p1",
                    "description": "Test Study"
                  }
                }
              ]
            }
            """;

        DatasetModel existingDataset = new DatasetModel();
        existingDataset.setDatasetId(1L);
        existingDataset.setRef("phs000001");

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetRepository.findByRef("phs000001")).thenReturn(Optional.of(existingDataset));

        assertDoesNotThrow(() -> fhirService.updateDatasetMetadata());
        verify(datasetMetadataRepository, never()).save(any());
    }

    @Test
    void testLogMetrics() {
        assertDoesNotThrow(() -> fhirService.logMetrics());
    }
}
