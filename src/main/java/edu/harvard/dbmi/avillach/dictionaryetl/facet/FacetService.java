package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.exceptions.CsvException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetFacetRefreshService;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.ConceptToFacetDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class FacetService {

    private final static Logger log = LoggerFactory.getLogger(FacetService.class);

    private final FacetRepository facetRepository;
    private final FacetCategoryService facetCategoryService;
    private final FacetConceptService facetConceptService;
    private final FacetMetadataRepository facetMetadataRepository;
    private final FacetConceptRepository facetConceptRepository;
    private final DatasetRepository datasetRepository;
    private final ConceptRepository conceptRepository;
    private final DatasetFacetRefreshService datasetFacetRefreshService;
    private final FacetCategoryRepository facetCategoryRepository;

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    public FacetService(FacetRepository facetRepository,
                        FacetCategoryService facetCategoryService,
                        FacetConceptService facetConceptService,
                        FacetMetadataRepository facetMetadataRepository,
                        FacetConceptRepository facetConceptRepository,
                        DatasetRepository datasetRepository,
                        ConceptRepository conceptRepository,
                        DatasetFacetRefreshService datasetFacetRefreshService,
                        FacetCategoryRepository facetCategoryRepository,
                        EntityManager entityManager) {
        this.facetRepository = facetRepository;
        this.facetCategoryService = facetCategoryService;
        this.facetConceptService = facetConceptService;
        this.facetMetadataRepository = facetMetadataRepository;
        this.facetConceptRepository = facetConceptRepository;
        this.datasetRepository = datasetRepository;
        this.conceptRepository = conceptRepository;
        this.datasetFacetRefreshService = datasetFacetRefreshService;
        this.facetCategoryRepository = facetCategoryRepository;
        this.entityManager = entityManager;
    }

    public FacetModel save(FacetModel facetModel) {
        return this.facetRepository.save(facetModel);
    }

    public Optional<FacetModel> findByName(String name) {
        return this.facetRepository.findByName(name);
    }

    /**
     * This method will create the data_type FacetCategory, continuous & categorical Facet, and map all continuous and
     * categorical Concepts to the respective Facet.
     */
    @Transactional
    public void createOrUpdateDefaultFacets() {
        // Ensure 'data_type' category exists
        FacetCategoryModel dataTypeCategoryModel = this.facetCategoryRepository.findByName("data_type")
                .orElseGet(() -> this.facetCategoryRepository.save(
                        new FacetCategoryModel("data_type", "Type of Variable", "Continuous or categorical")
                ));
        Long dataTypeFacetCategoryId = dataTypeCategoryModel.getFacetCategoryId();

        // Ensure categorical facet exists within data_type category
        FacetModel catFacet = facetRepository
                .findByNameAndFacetCategoryId("categorical", dataTypeFacetCategoryId)
                .orElseGet(() -> facetRepository.save(
                        new FacetModel(dataTypeFacetCategoryId, "categorical", "Categorical", "", null)
                ));
        Long catFacetId = catFacet.getFacetId();

        // Ensure continuous facet exists within data_type category
        FacetModel conFacet = facetRepository
                .findByNameAndFacetCategoryId("continuous", dataTypeFacetCategoryId)
                .orElseGet(() -> facetRepository.save(
                        new FacetModel(dataTypeFacetCategoryId, "continuous", "Continuous", "", null)
                ));
        Long conFacetId = conFacet.getFacetId();

        // Map concept types to facets (native queries are idempotent with ON CONFLICT DO NOTHING)
        facetConceptRepository.mapConceptConceptTypeToFacet(catFacetId, catFacet.getName());
        facetConceptRepository.mapConceptConceptTypeToFacet(conFacetId, conFacet.getName());
    }

    public List<String> getFacetMetadataKeyNames() {
        return this.facetMetadataRepository.getFacetMetadataKeyNames();
    }

    public List<String> getFacetNames() {
        return this.facetRepository.getAllFacetNames();
    }

    public List<FacetModel> findAllFacetsByDatasetIDs(Long[] datasetIDs) {
        return this.facetRepository.findAllFacetsByDatasetIDs(datasetIDs);
    }

    public List<FacetModel> findAll() {
        return this.facetRepository.findAll();
    }

    public ResponseEntity<List<FacetModel>> listAllResponse() {
        try {
            List<FacetModel> all = new ArrayList<>(facetRepository.findAll());
            if (all.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(all, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public Optional<FacetModel> findByID(Long parentId) {
        return this.facetRepository.findById(parentId);
    }

    public Optional<FacetMetadataModel> findFacetMetadataByFacetIDAndKey(Long facetId, String key) {
        return this.facetMetadataRepository.findByFacetIdAndKey(facetId, key);
    }

    // Controller-delegated methods moved from FacetController to service layer
    public ResponseEntity<FacetModel> updateFacet(String categoryName, String name, String display, String desc, String parentName) {
        Optional<FacetCategoryModel> facetCategoryModel = facetCategoryService.findByName(categoryName);
        Long categoryId;
        if (facetCategoryModel.isPresent()) {
            categoryId = facetCategoryModel.get().getFacetCategoryId();
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        Optional<FacetModel> parentModel = facetRepository.findByName(parentName);
        Long parentId = parentModel.map(FacetModel::getFacetId).orElse(null);
        Optional<FacetModel> facetData = facetRepository.findByName(name);

        if (facetData.isPresent()) {
            FacetModel existingFacet = facetData.get();
            existingFacet.setFacetCategoryId(categoryId);
            existingFacet.setName(name);
            existingFacet.setDisplay(display);
            existingFacet.setDescription(desc);
            existingFacet.setParentId(parentId);
            return new ResponseEntity<>(facetRepository.save(existingFacet), HttpStatus.OK);
        } else {
            try {
                FacetModel newFacet = facetRepository.save(new FacetModel(categoryId, name, display, desc, parentId));
                return new ResponseEntity<>(newFacet, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
    }

    public ResponseEntity<FacetModel> deleteFacetByName(String name) {
        Optional<FacetModel> facetData = facetRepository.findByName(name);
        if (facetData.isPresent()) {
            Long facetId = facetData.get().getFacetId();
            facetConceptRepository.deleteAllForFacetIds(List.of(facetId));
            facetRepository.delete(facetData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @Transactional
    public ResponseEntity<String> refreshBDCFacets() {
        // Delegate dataset-related refresh
        datasetFacetRefreshService.refreshDatasetFacet(true);
        // Ensure continuous/categorical facets and mappings
        createOrUpdateDefaultFacets();

        // Create/update data_source category and genomic facet, and map SAMPLE_ID concepts
        Optional<FacetCategoryModel> dataSourceCategoryModel = facetCategoryService.findByName("data_source");
        FacetCategoryModel dataSource;
        if (dataSourceCategoryModel.isEmpty()) {
            log.info("Did not find a 'data_source' category; Creating facet category...");
            dataSource = facetCategoryService.save(new FacetCategoryModel("data_source", "Data Type", "Associated metadata source"));
        } else {
            log.info("Facet Category found: {}", dataSourceCategoryModel.get());
             dataSource = dataSourceCategoryModel.get();
        }

        Long dataSourceFacetCategoryId = dataSource.getFacetCategoryId();
        // Find facet by name constrained to the data_source category to avoid duplicate insert
        FacetModel genomicSourceFacet = facetRepository
                .findByNameAndFacetCategoryId("data_source_genomic", dataSourceFacetCategoryId)
                .orElseGet(() -> save(new FacetModel(
                        dataSourceFacetCategoryId,
                        "data_source_genomic",
                        "Genomic",
                        "Associated with genomic data",
                        null))
                );
        Long genomicSourceFacetId = genomicSourceFacet.getFacetId();
        facetConceptRepository.mapConceptDisplayToFacet(genomicSourceFacetId, "SAMPLE_ID");
        return new ResponseEntity<>("Successfully updated default facets\n", HttpStatus.OK);
    }

    @Transactional
    public ResponseEntity<String> refreshDataTypeFacet() {
        try {
            createOrUpdateDefaultFacets();
            return new ResponseEntity<>("Successfully updated continuous and categorical data type facets\n", HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Transactional
    public ResponseEntity<FacetConceptModel> addFacetToFullDataset(String facetName, String datasetRef) {
        try {
            Optional<FacetModel> facet = facetRepository.findByName(facetName);
            var dataset = datasetRepository.findByRef(datasetRef);
            if (dataset.isEmpty() || facet.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
            Long datasetID = dataset.get().getDatasetId();
            Long facetId = facet.get().getFacetId();
            List<edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel> concepts = conceptRepository.findByDatasetId(datasetID);
            for (edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel concept : concepts) {
                Long conceptNodeId = concept.getConceptNodeId();
                Optional<FacetConceptModel> conceptFacet = facetConceptRepository.findByFacetIdAndConceptNodeId(facetId, conceptNodeId);
                if (conceptFacet.isEmpty()) {
                    facetConceptRepository.save(new FacetConceptModel(facetId, conceptNodeId));
                    if (facet.get().getParentId() != null) {
                        Optional<FacetModel> parentFacet = Optional.ofNullable(facetRepository.findByFacetId(facet.get().getParentId()));
                        if (parentFacet.isPresent()) {
                            Long parentFacetId = parentFacet.get().getFacetId();
                            Optional<FacetConceptModel> parentConceptFacet = facetConceptRepository.findByFacetIdAndConceptNodeId(parentFacetId, conceptNodeId);
                            if (parentConceptFacet.isEmpty()) {
                                facetConceptRepository.save(new FacetConceptModel(parentFacetId, conceptNodeId));
                            }
                        }
                    }
                }
            }
            return new ResponseEntity<>(null, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public ResponseEntity<List<FacetMetadataModel>> getAllFacetMetadataModels(Optional<String> name) {
        try {
            List<FacetMetadataModel> facetMetadataModels = new ArrayList<>();
            if (name.isEmpty()) {
                facetMetadataModels.addAll(facetMetadataRepository.findAll());
            } else {
                Long facetId = facetRepository.findByName(name.get()).get().getFacetId();
                facetMetadataModels.addAll(facetMetadataRepository.findByFacetId(facetId));
            }
            if (facetMetadataModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(facetMetadataModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Transactional
    public ResponseEntity<FacetMetadataModel> updateFacetMetadata(String name, String key, String values) {
        Optional<FacetModel> facet = facetRepository.findByName(name);
        if (facet.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        Long facetId = facet.get().getFacetId();
        Optional<FacetMetadataModel> facetMetadataData = facetMetadataRepository.findByFacetIdAndKey(facetId, key);
        try {
            if (facetMetadataData.isPresent()) {
                FacetMetadataModel existing = facetMetadataData.get();
                existing.setValue(values);
                return new ResponseEntity<>(facetMetadataRepository.save(existing), HttpStatus.OK);
            } else {
                FacetMetadataModel newFacetMetadata = facetMetadataRepository.save(new FacetMetadataModel(facetId, key, values));
                return new ResponseEntity<>(newFacetMetadata, HttpStatus.CREATED);
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Transactional
    public ResponseEntity<FacetMetadataModel> deleteFacetMetadata(String name, String key) {
        Long facetId = facetRepository.findByName(name).get().getFacetId();
        Optional<FacetMetadataModel> facetMetadataData = facetMetadataRepository.findByFacetIdAndKey(facetId, key);
        if (facetMetadataData.isPresent()) {
            facetMetadataRepository.delete(facetMetadataData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    public List<ConceptToFacetDTO> findFacetToConceptRelationshipsByDatasetID(Long datasetID) {
        return this.facetRepository.findFacetToConceptRelationshipsByDatasetID(datasetID);
    }

    /*Ingest facets from "Ideal Ingest" csv format
    Expected CSV headers:
    facet_category facet_name(unique) display_name description parent_name

    */
    @Transactional
    @PutMapping("/facet/csv")
    public ResponseEntity<String> updateFacetsFromCSVs(@RequestBody String input) {

        List<String[]> facets;
        Map<String, Integer> headerMap;
        List<String> metaColumnNames;
        RFC4180Parser csvParser = new RFC4180Parser();
        try (CSVReader reader = new CSVReaderBuilder(new StringReader(input)).withCSVParser(csvParser).build()) {
            String[] header = reader.readNext();
            headerMap = CSVUtility.buildCsvInputsHeaderMap(header);
            String[] coreFacetHeaders = {"facet_category", "facet_name(unique)", "display_name", "description", "parent_name"};
            metaColumnNames = CSVUtility.getExtraColumns(coreFacetHeaders, headerMap);
            if (metaColumnNames == null) {
                return new ResponseEntity<>(
                        "ERROR: Input headers for facets are not as expected. \n" +
                        "Verify that the following headers are present in the input csv file:\"facet_category\",\"facet_name(unique)\",\"display_name\",\"description\",\"parent_name\""
                        ,
                        HttpStatus.BAD_REQUEST);
            }
            facets = reader.readAll();
            facets.remove(header);
        } catch (IOException | CsvException e) {
            return new ResponseEntity<>(
                    "Error reading ingestion csv for facet categories. Error: \n" + Arrays.toString(e.getStackTrace()),
                    HttpStatus.BAD_REQUEST);
        }
        if (facets.isEmpty()) {
            return new ResponseEntity<>(
                    "No csv records found in facet categories input file.",
                    HttpStatus.BAD_REQUEST);
        }
        int facetUpdateCount = 0;
        int parentUpdateCount = 0;
        List<FacetModel> facetModels = new ArrayList<>();
        Map<String, Map<String, String>> metaMap = new HashMap<>();
        Map<String, String> parentMap = new HashMap<>();
        for (String[] facet : facets) {
            if (facet.length < headerMap.size())
                continue;
            String facetCategoryName = facet[headerMap.get("facet_category")];
            FacetCategoryModel facetCategory = facetCategoryService.findByName(facetCategoryName).orElseThrow(() -> new RuntimeException("No category found named " + facetCategoryName));
            String name = facet[headerMap.get("facet_name(unique)")];
            String display = facet[headerMap.get("display_name")];
            String description = facet[headerMap.get("description")];
            String parentName = facet[headerMap.get("parent_name")];
            if (!parentName.isEmpty())
                parentMap.put(name, parentName);
            FacetModel facetModel = new FacetModel(facetCategory.getFacetCategoryId(), name, display, description, null);
            facetModels.add(facetModel);
            Map<String, String> metaVals = new HashMap<>();
            for (String key : metaColumnNames) {
                String value = facet[headerMap.get(key)];
                if (!value.isBlank()) {
                    metaVals.put(key, value);
                }
            }
            metaMap.put(name, metaVals);
        }


        Query facetQuery = entityManager.createNativeQuery(getUpsertFacetQuery(facetModels));

        facetUpdateCount += facetQuery.executeUpdate();
        if(!parentMap.isEmpty()){
            Query parentQuery = entityManager.createNativeQuery(getUpdateParentIdsQuery(parentMap));
            parentUpdateCount += parentQuery.executeUpdate();
        }


        //    TODO ADD QUERY TO ADD METADATA FOR FACETS
        return new ResponseEntity<>("Successfully updated " + facetUpdateCount + " facets and associated " + parentUpdateCount + " parent facets\n", HttpStatus.OK);
    }


    public String getUpsertFacetQuery(List<FacetModel> facetModels) {
        String facetCategoryIds = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                facetModels.stream()
                                        .map(FacetModel::getFacetCategoryId)
                                        .collect(Collectors.toList()))
                                  + "])";

        String names ="UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                facetModels.stream()
                                        .map(model -> StringUtils.quote(model.getName()))
                                        .collect(Collectors.toList()))
                        + "])";
        String displays = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                facetModels.stream()
                                        .map(model -> StringUtils.quote(model.getDisplay()))
                                        .collect(Collectors.toList()))
                        + "])"
        ;
        String descriptions = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                facetModels.stream()
                                        .map(model -> StringUtils.quote(model.getDescription()))
                                        .collect(Collectors.toList()))
                        + "])";
        String vals = StringUtils.arrayToCommaDelimitedString(
                                        new String[] { facetCategoryIds, names, displays, descriptions });

        return "insert into facet (facet_category_id,name,display,description) "
               + "VALUES (" + vals + ")"
               + " ON CONFLICT (name, facet_category_id) DO UPDATE SET (display,description) = (EXCLUDED.display,EXCLUDED.description);";



    }

    public String getUpdateParentIdsQuery(Map<String, String> parentFacetMap) {
        String childNames = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                parentFacetMap.keySet().stream().map(StringUtils::quote)
                                                                  .collect(Collectors
                                                                                  .toList()))
                                  + "])";
                  String parentNames = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                                  parentFacetMap.values().stream().map(StringUtils::quote)
                                                  .collect(Collectors
                                                                  .toList()))
                                  + "])";

        return "with parent_table (child_name, parent_name) as"
     + "(select " + childNames + ", " + parentNames + ")"
     + "update facet set parent_id = parent_ref.p_id from "
     + "(select child_name, facet_id as p_id from parent_table left join facet on parent_name = name) as parent_ref"
     + " where name = child_name;";

    }

    public int deleteByName(String datasetRef) {
        return facetRepository.deleteByName(datasetRef);
    }
}
