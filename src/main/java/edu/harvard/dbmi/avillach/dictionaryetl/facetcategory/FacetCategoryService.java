package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.exceptions.CsvException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class FacetCategoryService {

    private final FacetCategoryRepository facetCategoryRepository;
    private final FacetCategoryMetaRepository facetCategoryMetaRepository;
    @PersistenceContext
            private EntityManager entityManager;
    @Autowired
    public FacetCategoryService(FacetCategoryRepository facetCategoryRepository, FacetCategoryMetaRepository facetCategoryMetaRepository) {
        this.facetCategoryRepository = facetCategoryRepository;
        this.facetCategoryMetaRepository = facetCategoryMetaRepository;
    }

    public FacetCategoryModel save(FacetCategoryModel facetCategoryModel) {
        return this.facetCategoryRepository.save(facetCategoryModel);
    }

    public Optional<FacetCategoryModel> findByName(String name) {
        return this.facetCategoryRepository.findByName(name);
    }

    public List<String> getFacetCategoryMetadataKeyNames() {
        return this.facetCategoryMetaRepository.getFacetCategoryMetadataKeyNames();
    }

    public List<FacetCategoryModel> findAll() {
        return this.facetCategoryRepository.findAll();
    }

    public List<FacetCategoryMeta> findFacetCategoryMetaByFacetCategoriesID(Long[] facetCategoryID) {
        return this.facetCategoryMetaRepository.findByFacetCategoryID(facetCategoryID);
    }

    public String getUpsertCategoryBatchQuery(List<FacetCategoryModel> catModels) {
    String names = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                                    catModels.stream()
                                                    .map(model -> StringUtils.quote(model.getName()))
                                                    .collect(Collectors.toList()))
                   + "])";
        String displays = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                                            catModels.stream()
                                                            .map(model -> StringUtils.quote(model.getDisplay()))
                                                            .collect(Collectors.toList()))
                               + "])";
        String descs = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                                            catModels.stream()
                                                            .map(model -> StringUtils.quote(model.getDescription()))
                                                            .collect(Collectors.toList()))
                               + "])";
        String vals = StringUtils.arrayToCommaDelimitedString(
                                       new String[] { names, displays, descs });
        return "insert into facet_category (name,display,description) "
               + "VALUES (" + vals + ")"
               + " ON CONFLICT (name) DO UPDATE SET (display,description) = (EXCLUDED.display,EXCLUDED.description);";

    }

    /*Ingest facet categories from "Ideal Ingest" csv format
        Expected CSV headers:
        name(unique)	display name	description
        */

        @Transactional

        public ResponseEntity<String> updateFacetCategoriesFromCSVs(@RequestBody String input) {
            List<String[]> facetCategories;
            Map<String, Integer> headerMap;
            List<String> metaColumnNames;
            RFC4180Parser csvParser = new RFC4180Parser();
            try (CSVReader reader = new CSVReaderBuilder(new StringReader(input)).withCSVParser(csvParser).build()) {
                String[] header = reader.readNext();
                headerMap = CSVUtility.buildCsvInputsHeaderMap(header);
                String[] coreCategoryHeaders = {"name(unique)", "display name", "description"};
                metaColumnNames = CSVUtility.getExtraColumns(coreCategoryHeaders, headerMap);
                if (metaColumnNames == null) {
                    return new ResponseEntity<>(
                            "ERROR: Input headers for facet categories are not as expected. \n" +
                            "Verify that the following headers are present in the input csv file: \"name(unique)\", \"display name\", \"description\""
                            ,
                            HttpStatus.BAD_REQUEST);
                }
                facetCategories = reader.readAll();
                facetCategories.remove(header);
            } catch (IOException | CsvException e) {
                return new ResponseEntity<>(
                        "Error reading ingestion csv for facet categories. Error: \n" + Arrays.toString(e.getStackTrace()),
                        HttpStatus.BAD_REQUEST);
            }
            if (facetCategories.isEmpty()) {
                return new ResponseEntity<>(
                        "No csv records found in facet categories input file.",
                        HttpStatus.BAD_REQUEST);
            }
            int catUpdateCount = 0;
            List<FacetCategoryModel> catModels = new ArrayList<>();
            Map<String, Map<String, String>> metaMap = new HashMap<>();
            for (String[] category : facetCategories) {
                if (category.length < headerMap.size())
                    continue;
                String name = category[headerMap.get("name(unique)")];
                String display = category[headerMap.get("display name")];
                String description = category[headerMap.get("description")];

                FacetCategoryModel newCategoryModel = new FacetCategoryModel(name, display, description);
                catModels.add(newCategoryModel);
                Map<String, String> metaVals = new HashMap<>();
                for (String key : metaColumnNames) {
                    String value = category[headerMap.get(key)];
                    if (!value.isBlank()) {
                        metaVals.put(key, value);
                    }
                }
                metaMap.put(name, metaVals);
            }
            Query categoryQuery = entityManager.createNativeQuery(getUpsertCategoryBatchQuery(catModels));

            catUpdateCount += categoryQuery.executeUpdate();

            //TODO add category metadata upsert query call here
            return new ResponseEntity<>("Successfully updated " + catUpdateCount + " facet categories \n", HttpStatus.OK);
        }
}
