package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.exceptions.CsvException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
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

    private final FacetRepository facetRepository;
    private final FacetCategoryService facetCategoryService;
    private final FacetConceptService facetConceptService;
    private final FacetMetadataRepository facetMetadataRepository;

    @PersistenceContext
            private EntityManager entityManager;

    @Autowired
    public FacetService(FacetRepository facetRepository, FacetCategoryService facetCategoryService, FacetConceptService facetConceptService, FacetMetadataRepository facetMetadataRepository) {
        this.facetRepository = facetRepository;
        this.facetCategoryService = facetCategoryService;
        this.facetConceptService = facetConceptService;
        this.facetMetadataRepository = facetMetadataRepository;
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
    public void createDefaultFacets() {
        FacetCategoryModel dataType = this.facetCategoryService.findByName("data_type").orElse(
                this.facetCategoryService.save(new FacetCategoryModel("data_type", "Type of Variable", "Continuous or categorical"))
        );

        FacetModel categorical = this.findByName("categorical").orElse(
                this.save(new FacetModel(dataType.getFacetCategoryId(), "categorical", "Categorical", "", null))
        );

        FacetModel continuous = this.findByName("continuous").orElse(
                this.save(new FacetModel(dataType.getFacetCategoryId(), "continuous", "Continuous", "", null))
        );

        this.facetConceptService.mapConceptConceptTypeToFacet(categorical.getFacetId(), "categorical");
        this.facetConceptService.mapConceptConceptTypeToFacet(continuous.getFacetId(), "continuous");
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

    public Optional<FacetModel> findByID(Long parentId) {
        return this.facetRepository.findById(parentId);
    }

    public Optional<FacetMetadataModel> findFacetMetadataByFacetIDAndKey(Long facetId, String key) {
        return this.facetMetadataRepository.findByFacetIdAndKey(facetId, key);
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
