package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatasetMetadataService {

    private final DatasetMetadataRepository datasetMetadataRepository;

    @Autowired
    public DatasetMetadataService(DatasetMetadataRepository datasetMetadataRepository) {
        this.datasetMetadataRepository = datasetMetadataRepository;
    }

    public List<DatasetMetadataModel> findByDatasetID(long datasetID) {
        return this.datasetMetadataRepository.findByDatasetId(datasetID);
    }

    /**
     *
     * @return A list of all the metadata set key values
     */
    public List<String> getAllKeyNames(Long[] datasetIDs) {
        return this.datasetMetadataRepository.findByDatasetID(datasetIDs);
    }
}
