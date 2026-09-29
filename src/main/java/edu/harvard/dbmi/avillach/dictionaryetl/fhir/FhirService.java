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
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.CodeableConcept;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Extension;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.Reference;
import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import io.micrometer.common.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class FhirService {

    private static final Logger logger = LoggerFactory.getLogger(FhirService.class);

    @Value("${fhir.api.bulk.fhir-page-size}")
    private int fhirPageSize = 200;

    @Value("${fhir.api.bulk.endpoint}")
    private String fhirBulkEndpoint;

    @Value("${fhir.url-to-key-map-json}")
    private String urlToKeyMapJson;

    @Value("${fhir.field-to-key-map-json:}")
    private String fieldToKeyMapJson;

    @Value("${fhir.webclient.max-in-memory-size:10485760}")
    private int webClientMaxInMemorySize;

    private static final String RESEARCH_STUDY_RESOURCE_TYPE = "ResearchStudy";
    private static final String OPERATION_OUTCOME_RESOURCE_TYPE = "OperationOutcome";
    private static final String MULTI_VALUE_SEPARATOR = "; ";

    /** Legacy: extension URL suffix (e.g. DBGAP-FHIR-Category) -> dataset_meta key. */
    private Map<String, String> urlToKeyMap = new HashMap<>();

    /** Standard ResearchStudy field name (category, sponsor, focus) -> dataset_meta key. */
    private Map<String, String> fieldToKeyMap = new HashMap<>();

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final DatasetRepository datasetRepository;
    private final DatasetMetadataRepository datasetMetadataRepository;

    private final Set<String> datasetsUpdated = new HashSet<>();
    private int metadataUpdated = 0;

    @Autowired
    public FhirService(
        WebClient.Builder webClientBuilder, ObjectMapper objectMapper, DatasetRepository datasetRepository,
        DatasetMetadataRepository datasetMetadataRepository, @Value("${fhir.api.base.url}") String fhirApiBaseUrl,
        @Value("${fhir.webclient.max-in-memory-size:10485760}") int maxInMemorySize
    ) {

        this.webClientMaxInMemorySize = maxInMemorySize;

        // Configure buffer size to handle large paginated responses
        ExchangeStrategies strategies = ExchangeStrategies.builder().codecs(codecs -> {
            codecs.defaultCodecs().maxInMemorySize(webClientMaxInMemorySize);
            codecs.defaultCodecs().jackson2JsonDecoder(new Jackson2JsonDecoder(objectMapper));
            codecs.defaultCodecs().jackson2JsonEncoder(new Jackson2JsonEncoder(objectMapper));
        }).build();

        this.webClient = webClientBuilder.baseUrl(fhirApiBaseUrl).exchangeStrategies(strategies).build();

        this.objectMapper = objectMapper;
        this.datasetRepository = datasetRepository;
        this.datasetMetadataRepository = datasetMetadataRepository;
    }

    @EventListener(ContextRefreshedEvent.class)
    protected void initKeyMaps() {
        if (StringUtils.isNotBlank(urlToKeyMapJson)) {
            setUrlToKeyMap(urlToKeyMapJson);
        } else {
            logger.warn("URL to Key Map JSON is null or empty. The map has not been initialized.");
        }

        if (StringUtils.isNotBlank(fieldToKeyMapJson)) {
            setFieldToKeyMap(fieldToKeyMapJson);
        } else {
            logger.warn("Field to Key Map JSON is null or empty. Standard ResearchStudy fields will not be mapped.");
        }
    }

    public void setUrlToKeyMap(String urlToKeyMapJson) {
        this.urlToKeyMap = parseKeyMap(urlToKeyMapJson, "URL to Key Map");
    }

    public void setFieldToKeyMap(String fieldToKeyMapJson) {
        this.fieldToKeyMap = parseKeyMap(fieldToKeyMapJson, "Field to Key Map");
    }

    private Map<String, String> parseKeyMap(String json, String label) {
        if (StringUtils.isBlank(json)) {
            logger.error("{} JSON is null or empty.", label);
            return new HashMap<>();
        }

        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (IOException e) {
            logger.error("Failed to parse {} JSON: {}", label, json, e);
            return new HashMap<>();
        }
    }

    @Transactional
    public void updateDatasetMetadata() throws IOException {
        datasetsUpdated.clear();
        metadataUpdated = 0;

        List<ResearchStudy> researchStudies = getResearchStudies();

        for (ResearchStudy researchStudy : researchStudies) {
            String refId = researchStudy.id().split("\\.")[0];
            Optional<DatasetModel> existingDatasetOpt = datasetRepository.findByRef(refId);

            // Proceed only if the dataset exists
            if (existingDatasetOpt.isPresent()) {
                DatasetModel existingDataset = existingDatasetOpt.get();

                updateDatasetDescription(existingDataset, researchStudy);
                addOrUpdateMetadata(existingDataset, researchStudy);
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

    private void addOrUpdateMetadata(DatasetModel dataset, ResearchStudy researchStudy) {
        for (Map.Entry<String, String> entry : resolveMetadataValues(researchStudy).entrySet()) {
            upsertMetadata(dataset, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Resolves the dataset_meta key/value pairs for a study. Legacy DBGAP-FHIR-* extensions are collected first (via {@code urlToKeyMap});
     * standard ResearchStudy fields (via {@code fieldToKeyMap}) then override them whenever they carry a non-blank value. The extension
     * value only survives when the standard field is absent.
     *
     * @param researchStudy the study
     * @return ordered map of dataset_meta key to value; never null
     */
    Map<String, String> resolveMetadataValues(ResearchStudy researchStudy) {
        Map<String, String> values = new LinkedHashMap<>();

        List<Extension> extensions = researchStudy.extension();
        if (extensions != null) {
            for (Extension extension : extensions) {
                if (extension == null || extension.url() == null) {
                    continue;
                }
                String key = extensionKeyFor(extension.url());
                if (key != null && StringUtils.isNotBlank(extension.valueString())) {
                    values.put(key, extension.valueString());
                }
            }
        }

        for (Map.Entry<String, String> mapping : fieldToKeyMap.entrySet()) {
            String value = standardFieldValue(researchStudy, mapping.getKey());
            if (StringUtils.isNotBlank(value) && StringUtils.isNotBlank(mapping.getValue())) {
                values.put(mapping.getValue(), value);
            }
        }

        return values;
    }

    private String extensionKeyFor(String url) {
        return urlToKeyMap.entrySet().stream().filter(entry -> url.endsWith(entry.getKey())).map(Map.Entry::getValue)
            .filter(StringUtils::isNotBlank).findFirst().orElse(null);
    }

    /**
     * Extracts the text value of a standard ResearchStudy field.
     *
     * @param researchStudy the study
     * @param fieldName one of {@code category}, {@code sponsor}, {@code focus}
     * @return the value or null when the field is absent or the name is not supported
     */
    static String standardFieldValue(ResearchStudy researchStudy, String fieldName) {
        if (fieldName == null) {
            return null;
        }
        return switch (fieldName) {
            case "category" -> joinCodeableConcepts(researchStudy.category());
            case "focus" -> joinCodeableConcepts(researchStudy.focus());
            case "sponsor" -> referenceToText(researchStudy.sponsor());
            default -> {
                logger.warn("Unsupported ResearchStudy field in field-to-key map: {}", fieldName);
                yield null;
            }
        };
    }

    /**
     * Joins the display values of a CodeableConcept list. Distinct non-blank values are joined with "; ".
     *
     * @param concepts the list, may be null
     * @return joined value or null when nothing usable is present
     */
    static String joinCodeableConcepts(List<CodeableConcept> concepts) {
        if (concepts == null || concepts.isEmpty()) {
            return null;
        }
        String joined = concepts.stream().filter(Objects::nonNull).map(CodeableConcept::displayValue).filter(StringUtils::isNotBlank)
            .distinct().collect(Collectors.joining(MULTI_VALUE_SEPARATOR));
        return joined.isEmpty() ? null : joined;
    }

    private static String referenceToText(Reference reference) {
        return reference == null ? null : reference.displayValue();
    }

    private void upsertMetadata(DatasetModel dataset, String key, String value) {
        datasetMetadataRepository.findByDatasetIdAndKey(dataset.getDatasetId(), key).ifPresentOrElse(existingMetadata -> {
            existingMetadata.setValue(value);
            metadataUpdated++;
        }, () -> {
            DatasetMetadataModel metadata = new DatasetMetadataModel(dataset.getDatasetId(), key, value);
            datasetMetadataRepository.save(metadata);
            metadataUpdated++;
        });
    }

    public List<ResearchStudy> getResearchStudies() throws IOException {
        List<ResearchStudy> out = new ArrayList<>();

        String nextUrl = withCountParam(fhirBulkEndpoint, fhirPageSize);

        while (nextUrl != null) {
            JsonNode bundle = fetchJson(nextUrl);

            JsonNode entries = bundle.path("entry");
            if (entries.isArray()) {
                for (JsonNode entry : entries) {
                    JsonNode resource = entry.path("resource");
                    if (resource.isMissingNode() || resource.isNull()) {
                        continue;
                    }

                    String resourceType = resource.path("resourceType").asText("");
                    if (!RESEARCH_STUDY_RESOURCE_TYPE.equals(resourceType)) {
                        if (OPERATION_OUTCOME_RESOURCE_TYPE.equals(resourceType)) {
                            logger.warn("Skipping OperationOutcome returned by the FHIR server: {}", resource.path("issue"));
                        } else {
                            logger.debug("Skipping non-ResearchStudy resource of type '{}'", resourceType);
                        }
                        continue;
                    }

                    out.add(objectMapper.treeToValue(resource, ResearchStudy.class));
                }
            }

            nextUrl = extractNextLink(bundle);
        }

        return out;
    }

    private JsonNode fetchJson(String url) throws IOException {
        String responseBody = webClient.get().uri(url).retrieve().bodyToMono(String.class).block();
        return objectMapper.readTree(responseBody);
    }

    private String extractNextLink(JsonNode bundle) {
        JsonNode links = bundle.path("link");
        if (links.isArray()) {
            for (JsonNode link : links) {
                if ("next".equals(link.path("relation").asText())) {
                    return link.path("url").asText(null);
                }
            }
        }
        return null;
    }

    private String withCountParam(String url, int count) {
        String sep = url.contains("?") ? "&" : "?";
        return url + sep + "_count=" + count;
    }

    public List<String> getDistinctPhsValues() throws IOException {
        List<ResearchStudy> researchStudies = getResearchStudies();
        Set<String> distinctPhsValues = researchStudies.stream().map(ResearchStudy::id).filter(id -> id != null && id.startsWith("phs"))
            .map(id -> id.split("\\.")[0]).collect(Collectors.toSet());

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
