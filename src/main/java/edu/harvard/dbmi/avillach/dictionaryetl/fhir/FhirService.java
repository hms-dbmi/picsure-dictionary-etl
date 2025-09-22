package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Bundle;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Entry;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Extension;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import io.micrometer.common.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class FhirService {

    private static final Logger logger = LoggerFactory.getLogger(FhirService.class);

    @Value("${fhir.api.bulk.endpoint}")
    private String fhirBulkEndpoint;

    @Value("${fhir.url-to-key-map-json}")
    private String urlToKeyMapJson;

    private Map<String, String> urlToKeyMap = new HashMap<>();

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final DatasetRepository datasetRepository;
    private final DatasetMetadataRepository datasetMetadataRepository;

    private final Set<String> datasetsUpdated = new HashSet<>();
    private int metadataUpdated = 0;

    @Autowired
    public FhirService(WebClient.Builder webClientBuilder, ObjectMapper objectMapper,
                       DatasetRepository datasetRepository,
                       DatasetMetadataRepository datasetMetadataRepository,
                       @Value("${fhir.api.base.url}") String fhirApiBaseUrl) {
        this.webClient = webClientBuilder.baseUrl(fhirApiBaseUrl).build();
        this.objectMapper = objectMapper;
        this.datasetRepository = datasetRepository;
        this.datasetMetadataRepository = datasetMetadataRepository;
    }

    @EventListener(ContextRefreshedEvent.class)
    protected void initUrlToKeyMap() {
        if (StringUtils.isNotBlank(urlToKeyMapJson)) {
            setUrlToKeyMap(urlToKeyMapJson);
        } else {
            logger.warn("URL to Key Map JSON is null or empty. The map has not been initialized.");
        }
    }

    public void setUrlToKeyMap(String urlToKeyMapJson) {
        if (StringUtils.isBlank(urlToKeyMapJson)) {
            logger.error("URL to Key Map JSON is null or empty.");
            this.urlToKeyMap = new HashMap<>();
            return;
        }

        try {
            this.urlToKeyMap = objectMapper.readValue(urlToKeyMapJson, new TypeReference<>() {
            });
        } catch (IOException e) {
            logger.error("Failed to parse URL to Key Map JSON: {}", urlToKeyMapJson, e);
            this.urlToKeyMap = new HashMap<>();
        }
    }

    @Transactional
    public void updateDatasetMetadata() throws IOException {
        List<ResearchStudy> researchStudies = getResearchStudies();

        for (ResearchStudy researchStudy : researchStudies) {
            String refId = researchStudy.id().split("\\.")[0];
            Optional<DatasetModel> existingDatasetOpt = datasetRepository.findByRef(refId);

            // Proceed only if the dataset exists
            if (existingDatasetOpt.isPresent()) {
                DatasetModel existingDataset = existingDatasetOpt.get();

                updateDatasetDescription(existingDataset, researchStudy);
                addOrUpdateMetadata(existingDataset, researchStudy.extension());
                datasetRepository.save(existingDataset);

                datasetsUpdated.add(refId);
            }
        }
        logMetrics();
    }

    private void updateDatasetDescription(DatasetModel dataset, ResearchStudy researchStudy) {
        String fhirDescription = researchStudy.description();
        if (!StringUtils.isBlank(fhirDescription)) {
            dataset.setDescription(fhirDescription);
        }
    }

    private void addOrUpdateMetadata(DatasetModel dataset, List<Extension> extensions) {
        if (extensions == null) return;

        for (Extension extension : extensions) {
            String key = urlToKeyMap.entrySet()
                    .stream()
                    .filter(entry -> extension.url().endsWith(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElse("");

            if (key.isBlank()) continue;

            datasetMetadataRepository.findByDatasetIdAndKey(dataset.getDatasetId(), key)
                    .ifPresentOrElse(
                            existingMetadata -> {
                                existingMetadata.setValue(extension.valueString());
                                metadataUpdated++;
                            },
                            () -> {
                                DatasetMetadataModel metadata = new DatasetMetadataModel(dataset.getDatasetId(), key, extension.valueString());
                                datasetMetadataRepository.save(metadata);
                                metadataUpdated++;
                            }
                    );
        }
    }

    public List<ResearchStudy> getResearchStudies() throws IOException {
        String responseBody = webClient.get()
                .uri(fhirBulkEndpoint)
                .retrieve()
                .bodyToMono(String.class)
                .block();

        Bundle bundle = objectMapper.readValue(responseBody, Bundle.class);
        return bundle.entry().stream()
                .map(Entry::resource)
                .toList();
    }

    public List<String> getDistinctPhsValues() throws IOException {
        List<ResearchStudy> researchStudies = getResearchStudies();
        Set<String> distinctPhsValues = researchStudies.stream()
                .map(ResearchStudy::id)
                .filter(id -> id != null && id.startsWith("phs"))
                .map(id -> id.split("\\.")[0])
                .collect(Collectors.toSet());

        return List.copyOf(distinctPhsValues);
    }

    public void logMetrics() {
        try {
            Map<String, Object> metrics = new HashMap<>();
            // Tracking metrics variables
            int newStudiesCreated = 0;
            metrics.put("Number of new studies created", newStudiesCreated);
            metrics.put("Number of datasets updated", datasetsUpdated.size());
            metrics.put("Total metadata updated", metadataUpdated);

            String jsonMetrics = objectMapper.writeValueAsString(metrics);
            logger.info(jsonMetrics);
        } catch (Exception e) {
            logger.error("Error while logging metrics", e);
        }
    }
}