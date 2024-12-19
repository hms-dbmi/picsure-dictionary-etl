package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
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

}
