package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import org.springframework.beans.factory.annotation.Value;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Bundle;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Entry;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Extension;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import io.micrometer.common.util.StringUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Service
public class FhirService {

    private static final Logger logger = Logger.getLogger(FhirService.class.getName());
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(FhirService.class);

    @Value("${fhir.api.base-url}")
    private String fhirApiBaseUrl;

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final DatasetRepository datasetRepository;
    private final DatasetMetadataRepository datasetMetadataRepository;

    @Autowired
    public FhirService(WebClient.Builder webClientBuilder, ObjectMapper objectMapper,
                       DatasetRepository datasetRepository,
                       DatasetMetadataRepository datasetMetadataRepository) {
        this.webClient = webClientBuilder.baseUrl(fhirApiBaseUrl).build();
        this.objectMapper = objectMapper;
        this.datasetRepository = datasetRepository;
        this.datasetMetadataRepository = datasetMetadataRepository;
    }

    private static final Map<String, String> URL_TO_KEY_MAP = Map.of(
            "DBGAP-FHIR-Category", "study_design",
            "DBGAP-FHIR-Sponsor", "sponsor",
            "DBGAP-FHIR-Focus", "study_focus"
    );

    // Method to retrieve ResearchStudies from the FHIR API
    public List<ResearchStudy> getResearchStudies() throws IOException {
        String url = "/fhir/ResearchStudy?_format=json&_count=10000";

        // Use WebClient to send GET request and process response asynchronously
        String responseBody = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String.class)
                .block();  // Blocking to make it synchronous (you can refactor to use async if needed)

        // Parse JSON into list of ResearchStudy objects
        Bundle bundle = objectMapper.readValue(responseBody, Bundle.class);
        return bundle.entry().stream()
                .map(Entry::resource)
                .toList();
    }

    // Method to transform the FHIR response and save/update it in the database
    @Transactional
    public void updateDatasetMetadata() throws IOException {
        List<ResearchStudy> researchStudies = getResearchStudies();

        for (ResearchStudy researchStudy : researchStudies) {
            // Extract the reference ID (only the part before the first '.')
            String refId = researchStudy.id().split("\\.")[0];

            // Log any resource IDs that are not "phs"
            //if (!refId.startsWith("phs")) {
            //    logger.warning("Skipping resource with non-phs ID: " + refId);
            //    continue;
            //}

            // Find the dataset by refId, or create a new one if it doesn't exist
            Optional<DatasetModel> existingDatasetOpt = datasetRepository.findByRef(refId);
            DatasetModel dataset;

            if (existingDatasetOpt.isPresent()) {
                // Existing dataset: only update specific fields
                dataset = existingDatasetOpt.get();

                // Update description if it doesn't exist or is blank
                String currentDescription = dataset.getDescription();
                //String newDescription = getExtensionValue(researchStudy, "study-description");
                String fhirDescription = researchStudy.description();
                //if (currentDescription == null || currentDescription.isBlank()) {
                dataset.setDescription(StringUtils.isBlank(fhirDescription) ? currentDescription: fhirDescription);

                // Update Metadata (extensions)
                List<Extension> extensions = researchStudy.extension();
                if (extensions != null) {
                    for (Extension extension : extensions) {
                        String url = extension.url();
                        String value = extension.valueString();

                        // Handle specific metadata mappings by checking if the URL ends with the key
                        if (url.endsWith("DBGAP-FHIR-Category") || url.endsWith("DBGAP-FHIR-Sponsor") || url.endsWith("DBGAP-FHIR-Focus")) {

                            final String key = URL_TO_KEY_MAP.entrySet()
                                    .stream()
                                    .filter(entry -> url.endsWith(entry.getKey()))
                                    .map(Map.Entry::getValue)
                                    .findFirst()
                                    .orElse("");

                            if(key.isBlank()) continue;
                            Optional<DatasetMetadataModel> existingMetadataOpt =
                                    datasetMetadataRepository.findByDatasetIdAndKey(dataset.getDatasetId(), key);

                            DatasetModel finalDataset = dataset;
                            DatasetMetadataModel metadata = existingMetadataOpt.orElseGet(() ->
                                    new DatasetMetadataModel(finalDataset.getDatasetId(), key, value));

                            metadata.setValue(value);

                            datasetMetadataRepository.save(metadata);
                        }
                    }
                }
            } else {
                // New dataset: set all fields

                dataset = new DatasetModel();
                dataset.setRef(refId);

                if(researchStudy.title() != null)  {
                    dataset.setFullName(researchStudy.title());
                } else {

                    dataset.setFullName("Missing researchStudy.getTitle()");
                    //System.out.println("FhirService: " + "Missing researchStudy.getTitle(): - " + refId + " : " + researchStudy.getId());
                    logger.warning("Missing researchStudy.getTitle(): - " + refId + " : " + researchStudy.id());
                    continue;
                }
                String fhirDescription = researchStudy.description();
                dataset.setDescription(fhirDescription);

                // cannot be null as column is non-null.
                // Would need to map abv from fhir or another source...
                dataset.setAbbreviation("");


                // Save the new dataset
                dataset = datasetRepository.save(dataset);

                // Add Metadata (extensions)
                List<Extension> extensions = researchStudy.extension();

                if (extensions != null) {
                    for (Extension extension : extensions) {
                        String url = extension.url();
                        String value = extension.valueString();

                        // Handle specific metadata mappings by checking if the URL ends with the key
                        if (url.endsWith("DBGAP-FHIR-Category") || url.endsWith("DBGAP-FHIR-Sponsor") || url.endsWith("DBGAP-FHIR-Focus")) {
                            String key = switch (url) {
                                case String s when s.endsWith("DBGAP-FHIR-Category") -> "study_design";
                                case String s when s.endsWith("DBGAP-FHIR-Sponsor") -> "sponsor";
                                case String s when s.endsWith("DBGAP-FHIR-Focus") -> "study_focus";
                                default -> "";
                            };
                            if(key.isBlank()) continue;
                            DatasetMetadataModel metadata = new DatasetMetadataModel(dataset.getDatasetId(), key, value);
                            datasetMetadataRepository.save(metadata);
                        }
                    }
                }
            }
        }
    }

    // Helper method to get specific extension value from ResearchStudy by matching URL suffix
    private String getExtensionValue(ResearchStudy researchStudy, String keySuffix) {
        return researchStudy.extension().stream()
                .filter(extension -> extension.url().endsWith(keySuffix))
                .findFirst()
                .map(Extension::valueString)
                .orElse(null);
    }

    public List<String> getDistinctPhsValues() throws IOException {
        List<ResearchStudy> researchStudies = getResearchStudies();

        // Extract distinct PHS values
        Set<String> distinctPhsValues = researchStudies.stream()
                .map(ResearchStudy::id)
                .filter(id -> id != null && id.startsWith("phs"))
                .map(id -> id.split("\\.")[0])
                .distinct()
                .collect(Collectors.toSet());

        return List.copyOf(distinctPhsValues);
    }

}
