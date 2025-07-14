package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import org.apache.commons.text.WordUtils;
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
    private final FacetCategoryRepository facetCategoryRepository;
    private final FacetRepository facetRepository;
    private final FacetConceptRepository facetConceptRepository;
    private final int BATCH_SIZE = 1000;
    private final Map<String, String> childParentPairs = new HashMap<>();

    public ConceptCSVService(ConceptRepository conceptRepository, ConceptMetadataRepository metadataRepository, FacetCategoryRepository facetCategoryRepository, FacetRepository facetRepository, FacetConceptRepository facetConceptRepository) {
        this.conceptRepository = conceptRepository;
        this.metadataRepository = metadataRepository;
        this.facetCategoryRepository = facetCategoryRepository;
        this.facetRepository = facetRepository;
        this.facetConceptRepository = facetConceptRepository;
    }

    public ConceptCSVManifest process(
        DatasetModel dataset,
          String csv,
          List<String> metaFacets,
          boolean conceptFacets,
          boolean categoryFacets
        ) {
        ConceptCSVIngestWrapper ingest =
            new ConceptCSVIngestWrapper(csv, dataset.getDatasetId(), conceptFacets, categoryFacets, metaFacets);
        List<ParsedCSVConceptRow> batch = new ArrayList<>();
        while (ingest.shouldContinue()) {
            Optional<ParsedCSVConceptRow> maybeConcept = ingest.next();
            if (maybeConcept.isEmpty()) {
                continue;
            }
            ParsedCSVConceptRow conceptAndMetas = maybeConcept.get();
            batch.add(conceptAndMetas);
            if (batch.size() >= BATCH_SIZE) {
                ingestConcepts(batch);
                batch = new ArrayList<>();
            }
        }
        if (!batch.isEmpty()) {
            ingestConcepts(batch);
        }
        ingestFacets(batch);
        return ingest.createManifest();
    }

    private void ingestFacets(List<ParsedCSVConceptRow> rows) {
        // we don't want to add facets if the row will not show up in search
        // concepts with no values meta are excluded, so filter those out
        rows = rows.stream()
            .filter(row -> row.metas()
                .stream()
                .anyMatch(meta -> "values".equals(meta.getFirst()) && StringUtils.hasLength(meta.getSecond()))
            )
            .toList();
        if (rows.isEmpty()) {
            return;
        }
        log.info("Creating facet categories");
        // take the first element and create or fetch each category
        Map<String, FacetCategoryModel> categories = rows.stream()
            .flatMap(row -> row.facetsAndPairs().stream())
            .map(FacetsAndPairs::category)
            .distinct()
            .map(name -> {
                Optional<FacetCategoryModel> maybeCat = facetCategoryRepository.findByName(name);
                return maybeCat.orElse(new FacetCategoryModel(name, createDisplay(name), ""));
            })
            .collect(Collectors.toMap(FacetCategoryModel::getName, Function.identity(), (cur, n) -> cur));
        facetCategoryRepository.saveAll(categories.values());
        log.info("{} facet categories created or fetched", categories.size());
        log.info("Creating or fetching facets");

        Map<String, FacetModel> facets = rows.stream()
            .flatMap(row -> row.facetsAndPairs().stream().map(FacetsAndPairs::facets))
            .distinct()
            .map(f -> createOrGetFacets(f, categories))
            .collect(Collectors.toMap(FacetModel::getName, Function.identity()));
        log.info("{} leaf facets created or fetched", facets.size());
        log.info("Associating concepts with facets");
        int pairCount = 0;
        for (ParsedCSVConceptRow row : rows) {
            for (FacetsAndPairs f : row.facetsAndPairs()) {// get the last facet, as that is the leaf that should be associated with the concept
                Long facetId = facets.get(f.facets().getLast().name()).getFacetId();
                facetConceptRepository.createFacetConceptForFacetAndConceptWithPath(facetId, f.conceptPath());
                pairCount++;
            }
        }
        log.info("{} facet concept pairs created", pairCount);
    }

    private FacetModel createOrGetFacets(List<NameDisplayCategory> facets, Map<String, FacetCategoryModel> categories) {
        FacetModel parent = null;
        for (NameDisplayCategory facet : facets) {
            Long categoryId = categories.get(facet.category()).getFacetCategoryId();
            FacetModel current = facetRepository.findByName(facet.name())
                .orElse(new FacetModel(categoryId, facet.name(), facet.display(), "", null));
            if (parent != null) {
                current.setParentId(parent.getFacetId());
            }
            parent = facetRepository.save(current);
        }
        return parent; // confusing, this is the leaf facet
    }


    private String createDisplay(String raw) {
        return WordUtils.capitalizeFully(raw.replaceAll("_", " "));
    }

    private void ingestConcepts(List<ParsedCSVConceptRow> batch) {
        log.info("Saving {} concepts", batch.size());
        Map<String, ConceptModel> savedMetas = conceptRepository.saveAll(batch.stream().map(ParsedCSVConceptRow::concept).toList())
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
