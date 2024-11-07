package edu.harvard.dbmi.avillach.dictionaryetl.anvil;

import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentModel;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class AnVILService {

    private final Logger logger = LoggerFactory.getLogger(AnVILService.class);
    private final DatasetRepository datasetRepository;
    private static final String ANVIL_PROJECT_BASE_LINK = "https://anvilproject.org/data/studies/";
    private final ConsentRepository consentRepository;
    private final DatasetMetadataRepository datasetMetadataRepository;

    public AnVILService(DatasetRepository datasetRepository, ConsentRepository consentRepository, DatasetMetadataRepository datasetMetadataRepository) {
        this.datasetRepository = datasetRepository;
        this.consentRepository = consentRepository;
        this.datasetMetadataRepository = datasetMetadataRepository;
    }

    public List<AnVILStudyMetadata> ingestAnVILData(String requestBody) {
        logger.info("ingestAnvilData() has started running.");
        List<AnVILStudyMetadata> anVILStudyMetadataList = this.serializeData(requestBody);
        List<String> newRefs = findExistingRefs(anVILStudyMetadataList);
        logger.info("Refs {}", newRefs);

        // Reduce the list of AnVIL Studies to the ones that don't exist in the database.
        List<AnVILStudyMetadata> metadataToAdd = anVILStudyMetadataList.stream().filter(metadata -> newRefs.contains(metadata.getPhsVal())).toList();
        metadataToAdd.forEach(anVILStudyMetadata -> {
            DatasetModel datasetModel = anVILStudyMetadata.generateDataset();
            ConsentModel consentModel = anVILStudyMetadata.generateConsent();
            List<DatasetMetadataModel> datasetMetadataModels = anVILStudyMetadata.generateDatasetMetadata();

            DatasetModel persistedDataset = datasetRepository.save(datasetModel);

            consentModel.setDatasetId(persistedDataset.getDatasetId());
            consentRepository.save(consentModel);

            datasetMetadataModels.forEach(metadata -> metadata.setDatasetId(persistedDataset.getDatasetId()));
            datasetMetadataRepository.saveAll(datasetMetadataModels);
        });

        logger.info("ingestAnvilData() has completed running.");
        return metadataToAdd;
    }

    protected List<String> findExistingRefs(List<AnVILStudyMetadata> anVILStudyMetadataList) {
        // Which studies already exist
        List<String> refs = anVILStudyMetadataList.stream().map(AnVILStudyMetadata::getPhsVal).toList();

        // Get the list of refs not in the database.
        // I am converting refs to an array because the PostgreSQL expects an array to unnest.
        return this.datasetRepository.findValuesNotInRef(refs.toArray(String[]::new));
    }

    /**
     * @param requestBody The contents of an AnVIL Studies Dataset.
     * @return A list of AnVILStudyMetadata
     */
    protected List<AnVILStudyMetadata> serializeData(String requestBody) {
        String[] split = requestBody.split("\n");

        String[] firstRow = split[0].split("\t");
        Map<String, Integer> headers = new HashMap<>();
        for (int i = 0; i < firstRow.length; i++) {
            headers.put(firstRow[i], i);
        }

        String[] data = Arrays.copyOfRange(split, 1, split.length);
        return Arrays.stream(data).map(metadata -> getAnVILStudyMetadata(metadata, headers)).toList();
    }

    /**
     * Serialize a single line of the AnVIL Study Dataset contents into the AnVIL StudyMetadata Module
     *
     * @param line    A line of data from the AnVIL Study Dataset contents
     * @param headers A Map of the Header values to their index (E.g. (Abbreviate, 0))
     * @return Returns a serialized AnVILStudyMetadata
     */
    private static AnVILStudyMetadata getAnVILStudyMetadata(String line, Map<String, Integer> headers) {
        String[] values = line.split("\t");
        AnVILStudyMetadata model = new AnVILStudyMetadata();
        model.setParticipants(values[headers.get("Participants")]);
        model.setAccession(values[headers.get("Accession")]);
        model.setAbbreviation(values[headers.get("Abbreviation")]);
        model.setName(values[headers.get("Name")]);
        model.setClinicalVariables(values[headers.get("Clinical Variables")]);
        model.setSamplesSequenced(values[headers.get("Samples sequenced")]);
        model.setPhsVal(extractPhsVal(model.getAccession()));
        model.setStudyFocus(values[headers.get("Study Focus")]);
        model.setLink(ANVIL_PROJECT_BASE_LINK + model.getPhsVal());

        return model;
    }

    private static String extractPhsVal(String accession) {
        return accession.split("\\.")[0];
    }


}
