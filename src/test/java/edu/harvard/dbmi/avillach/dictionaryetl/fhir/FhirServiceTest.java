package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.CodeableConcept;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Coding;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import org.mockito.ArgumentCaptor;
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
import java.util.Map;
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
            webClientBuilder, objectMapper, datasetRepository, datasetMetadataRepository, "https://test-fhir-api.example.com",
            10 * 1024 * 1024 // 10MB buffer
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

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(page1Json)).thenReturn(Mono.just(page2Json));

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

        verify(datasetRepository).save(argThat(dataset -> "Updated Description".equals(dataset.getDescription())));
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
        when(datasetMetadataRepository.findByDatasetIdAndKey(1L, "Study Accession")).thenReturn(Optional.of(existingMetadata));

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

    // ---------------------------------------------------------------------------------------------
    // ALS-12872: standard ResearchStudy fields (category / sponsor / focus) and new-server behaviour
    // ---------------------------------------------------------------------------------------------

    private static final String FIELD_MAP_JSON = """
        {"category":"study_design","sponsor":"sponsor","focus":"study_focus"}
        """;

    private static final String EXTENSION_MAP_JSON = """
        {"DBGAP-FHIR-Category":"study_design","DBGAP-FHIR-Sponsor":"sponsor","DBGAP-FHIR-Focus":"study_focus"}
        """;

    private static final String EXTENSION_BASE = "https://h1vyzwgoo1.execute-api.us-east-1.amazonaws.com/staging/StructureDefinition/";

    private DatasetModel stubDataset(String ref) {
        DatasetModel dataset = new DatasetModel();
        dataset.setDatasetId(1L);
        dataset.setRef(ref);
        when(datasetRepository.findByRef(ref)).thenReturn(Optional.of(dataset));
        return dataset;
    }

    private Map<String, String> savedMetadata() {
        ArgumentCaptor<DatasetMetadataModel> captor = ArgumentCaptor.forClass(DatasetMetadataModel.class);
        verify(datasetMetadataRepository, atLeast(0)).save(captor.capture());
        Map<String, String> byKey = new java.util.LinkedHashMap<>();
        captor.getAllValues().forEach(m -> byKey.put(m.getKey(), m.getValue()));
        return byKey;
    }

    @Test
    void testGetResearchStudies_ParsesStandardFields() throws IOException {
        // Shape verified against the production server; includes fields the model does not declare.
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "type": "searchset",
              "entry": [
                {
                  "resource": {
                    "resourceType": "ResearchStudy",
                    "id": "phs000007.v35.p16.c1",
                    "meta": {"versionId": "1", "lastUpdated": "2026-09-21T12:56:59.486886898Z"},
                    "identifier": [{"value": "phs000007.v35.p16"}],
                    "title": "Framingham Cohort",
                    "status": "completed",
                    "category": [{"text": "Prospective Longitudinal Cohort"}],
                    "focus": [{"text": "Cardiovascular Diseases"}],
                    "condition": [{"text": "Heart Diseases"}],
                    "keyword": [{"text": "cohort"}],
                    "description": "Framingham description",
                    "sponsor": {"display": "National Heart, Lung, and Blood Institute"},
                    "extension": [
                      {"url": "%sDBGAP-FHIR-Category", "valueString": "Prospective Longitudinal Cohort", "valueCode": "x"}
                    ]
                  },
                  "search": {"mode": "match"}
                }
              ]
            }
            """.formatted(EXTENSION_BASE);

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));

        List<ResearchStudy> results = fhirService.getResearchStudies();

        assertEquals(1, results.size());
        ResearchStudy study = results.get(0);
        assertEquals("phs000007.v35.p16.c1", study.id());
        assertEquals("Prospective Longitudinal Cohort", study.category().get(0).text());
        assertEquals("Cardiovascular Diseases", study.focus().get(0).text());
        assertEquals("National Heart, Lung, and Blood Institute", study.sponsor().display());
        assertEquals(1, study.extension().size());
    }

    @Test
    void testGetResearchStudies_SkipsOperationOutcomeEntry() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "type": "searchset",
              "entry": [
                {
                  "resource": {"resourceType": "ResearchStudy", "id": "phs000001.v1.p1.c1"},
                  "search": {"mode": "match"}
                },
                {
                  "resource": {
                    "resourceType": "OperationOutcome",
                    "issue": [{"severity": "warning", "code": "processing",
                               "diagnostics": "The search result parameter _format is not supported by this server."}]
                  },
                  "search": {"mode": "outcome"}
                }
              ]
            }
            """;

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));

        List<ResearchStudy> results = fhirService.getResearchStudies();

        assertEquals(1, results.size());
        assertEquals("phs000001.v1.p1.c1", results.get(0).id());
    }

    @Test
    void testGetResearchStudies_FollowsOpaquePageTokenNextLink() throws IOException {
        String nextUrl =
            "https://h4nez3yyb6.execute-api.us-east-1.amazonaws.com/prod/ResearchStudy?_count=100&page=AAMA-EFRSURBSGd2bzlHTkh2NkZNeXNGMkNKelNQaUp6aCtWTXNCNXh6R0owVG0xQ1RtRzZRSHFIMnVNdXZmL0VXRGV0ZW03R0xoRkFBQUFmakI4QmdrcWhraUc5dzBCQndhZ2J6QnRBZ0VBTUdnR0NTcUdTSWIzRFFFSEFUQWVCZ2xnaGtnQlpRTUVBUzR3RVFRTTFHV2FoMlp4dW9aejdBbUxBZ0VRZ0RzK1pac1AySERYcjZFWVB2dmxTZ0Z2VGVNTEw2cXFNcU03aG9PdGR2QzFUUDFmMTFrM2pPbUorWDBJNnNNMHVhSG8yUUUxWnZITzg3RWFrUT09mjXYj7xt5lXY_NwHOPtQS7kELgDSLWTy4CcRWtw1cH2CWBc5xYm5QyU07aGQ_XuuBQUE1xVu63VelbJ_5yTafurash4FjphJSC952gbFEoV6v7FdjM2ogoHpCXkNR9pRfAM=";
        String page1Json = """
            {
              "resourceType": "Bundle",
              "type": "searchset",
              "link": [{"relation": "next", "url": "%s"}],
              "entry": [{"resource": {"resourceType": "ResearchStudy", "id": "phs000001.v1.p1.c1"}}]
            }
            """.formatted(nextUrl);
        String page2Json = """
            {
              "resourceType": "Bundle",
              "type": "searchset",
              "entry": [{"resource": {"resourceType": "ResearchStudy", "id": "phs000002.v1.p1.c1"}}]
            }
            """;

        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(page1Json)).thenReturn(Mono.just(page2Json));

        List<ResearchStudy> results = fhirService.getResearchStudies();

        assertEquals(2, results.size());
        verify(requestHeadersUriSpec).uri("/ResearchStudy?_count=500");
        verify(requestHeadersUriSpec).uri(nextUrl);
        verify(webClient, times(2)).get();
    }

    @Test
    void testUpdateDatasetMetadata_StandardFieldsWriteThreeKeys() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [{"resource": {
                "resourceType": "ResearchStudy",
                "id": "phs000007.v35.p16.c1",
                "category": [{"text": "Prospective Longitudinal Cohort"}],
                "focus": [{"text": "Cardiovascular Diseases"}],
                "sponsor": {"display": "National Heart, Lung, and Blood Institute"}
              }}]
            }
            """;
        stubDataset("phs000007");
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetMetadataRepository.findByDatasetIdAndKey(anyLong(), anyString())).thenReturn(Optional.empty());
        fhirService.setFieldToKeyMap(FIELD_MAP_JSON);
        fhirService.setUrlToKeyMap(EXTENSION_MAP_JSON);

        fhirService.updateDatasetMetadata();

        Map<String, String> saved = savedMetadata();
        assertEquals(3, saved.size());
        assertEquals("Prospective Longitudinal Cohort", saved.get("study_design"));
        assertEquals("Cardiovascular Diseases", saved.get("study_focus"));
        assertEquals("National Heart, Lung, and Blood Institute", saved.get("sponsor"));
    }

    @Test
    void testUpdateDatasetMetadata_StandardFieldOverridesExtension() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [{"resource": {
                "resourceType": "ResearchStudy",
                "id": "phs000001.v1.p1.c1",
                "category": [{"text": "Standard Category"}],
                "focus": [{"text": "Standard Focus"}],
                "sponsor": {"display": "Standard Sponsor"},
                "extension": [
                  {"url": "%sDBGAP-FHIR-Category", "valueString": "Extension Category"},
                  {"url": "%sDBGAP-FHIR-Focus", "valueString": "Extension Focus"},
                  {"url": "%sDBGAP-FHIR-Sponsor", "valueString": "Extension Sponsor"}
                ]
              }}]
            }
            """.formatted(EXTENSION_BASE, EXTENSION_BASE, EXTENSION_BASE);
        stubDataset("phs000001");
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetMetadataRepository.findByDatasetIdAndKey(anyLong(), anyString())).thenReturn(Optional.empty());
        fhirService.setFieldToKeyMap(FIELD_MAP_JSON);
        fhirService.setUrlToKeyMap(EXTENSION_MAP_JSON);

        fhirService.updateDatasetMetadata();

        Map<String, String> saved = savedMetadata();
        assertEquals(3, saved.size());
        assertEquals("Standard Category", saved.get("study_design"));
        assertEquals("Standard Focus", saved.get("study_focus"));
        assertEquals("Standard Sponsor", saved.get("sponsor"));
    }

    @Test
    void testUpdateDatasetMetadata_FallsBackToExtensionWhenStandardFieldMissing() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [{"resource": {
                "resourceType": "ResearchStudy",
                "id": "phs000001.v1.p1.c1",
                "category": [{"text": "Standard Category"}],
                "sponsor": {"display": "Standard Sponsor"},
                "extension": [
                  {"url": "%sDBGAP-FHIR-Category", "valueString": "Extension Category"},
                  {"url": "%sDBGAP-FHIR-Focus", "valueString": "Extension Focus"},
                  {"url": "%sDBGAP-FHIR-Sponsor", "valueString": "Extension Sponsor"}
                ]
              }}]
            }
            """.formatted(EXTENSION_BASE, EXTENSION_BASE, EXTENSION_BASE);
        stubDataset("phs000001");
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetMetadataRepository.findByDatasetIdAndKey(anyLong(), anyString())).thenReturn(Optional.empty());
        fhirService.setFieldToKeyMap(FIELD_MAP_JSON);
        fhirService.setUrlToKeyMap(EXTENSION_MAP_JSON);

        fhirService.updateDatasetMetadata();

        Map<String, String> saved = savedMetadata();
        assertEquals(3, saved.size());
        assertEquals("Standard Category", saved.get("study_design"));
        assertEquals("Extension Focus", saved.get("study_focus"));
        assertEquals("Standard Sponsor", saved.get("sponsor"));
    }

    @Test
    void testUpdateDatasetMetadata_MissingOptionalFieldsWritesOnlyPresentKeys() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [{"resource": {
                "resourceType": "ResearchStudy",
                "id": "phs000001.v1.p1.c1",
                "category": [{"text": "Only Category"}]
              }}]
            }
            """;
        stubDataset("phs000001");
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetMetadataRepository.findByDatasetIdAndKey(anyLong(), anyString())).thenReturn(Optional.empty());
        fhirService.setFieldToKeyMap(FIELD_MAP_JSON);
        fhirService.setUrlToKeyMap(EXTENSION_MAP_JSON);

        assertDoesNotThrow(() -> fhirService.updateDatasetMetadata());

        Map<String, String> saved = savedMetadata();
        assertEquals(Map.of("study_design", "Only Category"), saved);
    }

    @Test
    void testUpdateDatasetMetadata_StandardFieldUpdatesExistingRow() throws IOException {
        String bundleJson = """
            {
              "resourceType": "Bundle",
              "entry": [{"resource": {
                "resourceType": "ResearchStudy",
                "id": "phs000001.v1.p1.c1",
                "sponsor": {"display": "New Sponsor"}
              }}]
            }
            """;
        stubDataset("phs000001");
        DatasetMetadataModel existing = new DatasetMetadataModel(1L, "sponsor", "Old Sponsor");
        when(responseSpec.bodyToMono(String.class)).thenReturn(Mono.just(bundleJson));
        when(datasetMetadataRepository.findByDatasetIdAndKey(1L, "sponsor")).thenReturn(Optional.of(existing));
        fhirService.setFieldToKeyMap(FIELD_MAP_JSON);

        fhirService.updateDatasetMetadata();

        assertEquals("New Sponsor", existing.getValue());
        verify(datasetMetadataRepository, never()).save(any());
    }

    @Test
    void testJoinCodeableConcepts_TextThenCodingDisplayDistinctJoined() {
        List<CodeableConcept> concepts = List.of(
            new CodeableConcept(null, "Alpha"), new CodeableConcept(List.of(new Coding("sys", "b", "Beta")), null),
            new CodeableConcept(List.of(new Coding("sys", "a", "Alpha")), "  "), new CodeableConcept(null, null)
        );

        assertEquals("Alpha; Beta", FhirService.joinCodeableConcepts(concepts));
        assertNull(FhirService.joinCodeableConcepts(null));
        assertNull(FhirService.joinCodeableConcepts(List.of()));
        assertNull(FhirService.joinCodeableConcepts(List.of(new CodeableConcept(null, ""))));
    }

    @Test
    void testStandardFieldValue_UnsupportedFieldReturnsNull() {
        ResearchStudy study =
            new ResearchStudy("ResearchStudy", "phs1", null, null, null, null, List.of(new CodeableConcept(null, "Cat")), null, null);

        assertEquals("Cat", FhirService.standardFieldValue(study, "category"));
        assertNull(FhirService.standardFieldValue(study, "focus"));
        assertNull(FhirService.standardFieldValue(study, "sponsor"));
        assertNull(FhirService.standardFieldValue(study, "keyword"));
        assertNull(FhirService.standardFieldValue(study, null));
    }

    @Test
    void testSetFieldToKeyMap_ValidJson() {
        assertDoesNotThrow(() -> fhirService.setFieldToKeyMap(FIELD_MAP_JSON));
    }

    @Test
    void testSetFieldToKeyMap_InvalidJson() {
        assertDoesNotThrow(() -> fhirService.setFieldToKeyMap("{ invalid json }"));
    }

    @Test
    void testSetFieldToKeyMap_NullJson() {
        assertDoesNotThrow(() -> fhirService.setFieldToKeyMap(null));
    }

    @Test
    void testSetFieldToKeyMap_EmptyJson() {
        assertDoesNotThrow(() -> fhirService.setFieldToKeyMap(""));
    }
}
