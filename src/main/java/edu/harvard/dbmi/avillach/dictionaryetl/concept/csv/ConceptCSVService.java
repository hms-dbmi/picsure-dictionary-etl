package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ConceptCSVService {

    private static final Logger log = LoggerFactory.getLogger(ConceptCSVService.class);
    private final ConceptRepository conceptRepository;
    private final ConceptMetadataRepository metadataRepository;
    private final int BATCH_SIZE = 100;
    private final Map<String, String> childParentPairs = new HashMap<>();

    public ConceptCSVService(ConceptRepository conceptRepository, ConceptMetadataRepository metadataRepository) {
        this.conceptRepository = conceptRepository;
        this.metadataRepository = metadataRepository;
    }

    public ConceptCSVManifest process(DatasetModel dataset, String csv) {
        ConceptCSVIngestWrapper ingest = new ConceptCSVIngestWrapper(csv, dataset.getDatasetId());
        List<ConceptAndMetas> batch = new ArrayList<>();
        while (ingest.shouldContinue()) {
            Optional<ConceptAndMetas> maybeConcept = ingest.next();
            if (maybeConcept.isEmpty()) {
                continue;
            }
            ConceptAndMetas conceptAndMetas = maybeConcept.get();
            batch.add(conceptAndMetas);
            if (batch.size() >= BATCH_SIZE) {
                ingestConcepts(batch);
                batch = new ArrayList<>();
            }
        }
        if (!batch.isEmpty()) {
            ingestConcepts(batch);
        }
        return ingest.createManifest();
    }

    private void ingestConcepts(List<ConceptAndMetas> batch) {
        log.info("Saving {} concepts", batch.size());
        Map<String, ConceptModel> savedMetas = conceptRepository.saveAll(batch.stream().map(ConceptAndMetas::concept).toList())
            .stream()
            .collect(Collectors.toMap(ConceptModel::getConceptPath, Function.identity()));
        log.info("Done saving concepts");
        List<ConceptMetadataModel> metas = batch.stream()
            .flatMap(conceptAndMetas -> conceptAndMetas.metas().stream().map(meta -> {
                ConceptMetadataModel model = new ConceptMetadataModel();
                model.setKey(meta.getFirst());
                model.setValue(meta.getSecond());
                model.setConceptNodeId(savedMetas.get(conceptAndMetas.concept().getConceptPath()).getConceptNodeId());
                return model;
            }))
            .toList();
        log.info("Saving {} metas", metas.size());
        metadataRepository.saveAll(metas);
        log.info("Saved metas");
        log.info("Adding to child parent pairs");
        batch.stream()
            .filter(c -> StringUtils.hasLength(c.parentPath()))
            .forEach(c -> childParentPairs.put(c.concept().getConceptPath(), c.parentPath()));
        log.info("Done initial ingest");
    }

    @Transactional
    public void linkConceptNodes(){
        childParentPairs.forEach(conceptRepository::updateConceptParentIds);
        conceptRepository.flush();
    }
}
