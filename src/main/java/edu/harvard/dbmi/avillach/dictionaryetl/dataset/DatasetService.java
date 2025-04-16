package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class DatasetService {

    private final DatasetRepository datasetRepository;

    @Autowired
    public DatasetService(DatasetRepository datasetRepository) {
        this.datasetRepository = datasetRepository;
    }

    public DatasetModel save(DatasetModel datasetModel) {
        return this.datasetRepository.save(datasetModel);
    }

    public Optional<DatasetModel> findByRef(String datasetRef) {
        return this.datasetRepository.findByRef(datasetRef);
    }

    public void deleteAll() {
        this.datasetRepository.deleteAll();
    }

    public List<DatasetModel> findAll() {
        return this.datasetRepository.findAll();
    }
    public Map<String, Integer> buildCsvInputsHeaderMap(String[] headers) {
                Map<String, Integer> inputsHeaders = new HashMap<String, Integer>();
                for (int i = 0; i < headers.length; i++) {
                        inputsHeaders.put(headers[i], i);
                }
                return inputsHeaders;
    }


    public Optional<DatasetModel> findByID(Long datasetId) {
        if (datasetId == null) {
            return Optional.empty();
        }
        return this.datasetRepository.findById(datasetId);
    }

    public List<DataSetRefDto> getAllDatasetRefsSorted() {
        return this.datasetRepository.getAllDatasetRefsSorted();
    }

    public List<DataSetRefDto> getDatasetRefsSorted(String[] datasetRefs) {
        return this.datasetRepository.getDatasetRefsSorted(datasetRefs);
    }
}
