package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import com.opencsv.CSVReader;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@Service
public class FacetConceptService {

    private final FacetConceptRepository facetConceptRepository;
    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    FacetRepository facetRepository;

    @Autowired
    public FacetConceptService(FacetConceptRepository facetConceptRepository) {
        this.facetConceptRepository = facetConceptRepository;
    }

    public void mapConceptConceptTypeToFacet(Long facetID, String conceptType) {
        this.facetConceptRepository.mapConceptConceptTypeToFacet(facetID, conceptType);
    }

    public Optional<FacetConceptModel> findByFacetAndConcept(Long facetID, Long conceptID) {
        return this.facetConceptRepository.findByFacetIdAndConceptNodeId(facetID, conceptID);
    }

    @Transactional
    public ResponseEntity<String> updateFacetConceptMappingsFromCSVs(@RequestBody String input) {
        List<String[]> conceptMappings;
        Map<String, Integer> headerMap;
        try (CSVReader reader = new CSVReader(new StringReader(input))) {
            String[] header = reader.readNext();
            headerMap = CSVUtility.buildCsvInputsHeaderMap(header);
            conceptMappings = reader.readAll();
            conceptMappings.remove(header);
        } catch (IOException | CsvException e) {
            return new ResponseEntity<>(
                    "Error reading ingestion csv for facet_concept mappings. Error: \n" + Arrays.toString(e.getStackTrace()),
                    HttpStatus.BAD_REQUEST);
        }
        List<String[]> finalConceptMappings = conceptMappings;
        AtomicInteger updateCount = new AtomicInteger();
        headerMap.forEach((facetName, colIndex) -> {
                    FacetModel facet = facetRepository.findByName(facetName).orElseThrow(
                            () -> new RuntimeException("No facet found named " + facetName)
                    );
                    List<String> conceptPaths = new ArrayList<>();
                    // Collect column values
                    for (String[] record : finalConceptMappings) {
                        if (colIndex < record.length && !record[colIndex].isEmpty()) {
                            String path = record[colIndex];
                            //adding catch for if people forget the ending backslash
                            path = path.endsWith("\\") ? path : path + "\\";
                            conceptPaths.add(path);
                        }
                    }
                    updateCount.addAndGet(addColumnToFacet(facet,conceptPaths));
                }
        );


        return new ResponseEntity<>("Successfully updated " + updateCount + "facet/concept mappings\n", HttpStatus.OK);
    }

    public int addColumnToFacet(FacetModel facet, List<String> conceptPaths) {
        int updateCount = 0;
        Long facetId = facet.getFacetId();
        Query facetConceptQuery = entityManager.createNativeQuery(getUpdateConceptFacetsByListQuery(facetId, conceptPaths));
        updateCount += (facetConceptQuery.executeUpdate());
        if (facet.getParentId() != null) {
            updateCount += addColumnToFacet(facetRepository.findByFacetId(facet.getParentId()), conceptPaths);
        }
        return updateCount;

    }


    public String getUpdateConceptFacetsByListQuery(Long facetId, List<String> conceptPaths) {
        String conceptClause = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptPaths.stream().map(StringUtils::quote)
                        .collect(Collectors
                                .toList()))
                               + "])";
        String insertQuery = "INSERT INTO dict.facet__concept_node (concept_node_id, facet_id) " +
                             "SELECT concept_node.concept_node_id, " + facetId + " as facetId " +
                             "FROM dict.concept_node " +
                             "WHERE concept_node.concept_path in ( select " + conceptClause +
                             ") ON CONFLICT (concept_node_id, facet_id) DO NOTHING";
        System.out.println(insertQuery);
        return insertQuery;
    }

}
