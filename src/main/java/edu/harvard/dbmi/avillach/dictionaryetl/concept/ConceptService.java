package edu.harvard.dbmi.avillach.dictionaryetl.concept;


import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import jakarta.persistence.EntityManager;
import java.util.*;
import java.util.stream.Collectors;

import javax.sql.DataSource;

@Service
public class ConceptService {

    private final ConceptRepository conceptRepository;
    private static final Logger log = LoggerFactory.getLogger(ConceptService.class);
    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    public ConceptService(ConceptRepository conceptRepository) {
        this.conceptRepository = conceptRepository;
    }

    @Autowired
    ConceptMetadataRepository conceptMetadataRepository;

    public ConceptModel save(ConceptModel conceptModel) {
        return this.conceptRepository.save(conceptModel);
    }

    public Optional<ConceptModel> findByConcept(String conceptPath) {
        return this.conceptRepository.findByConceptPath(conceptPath);
    }

    public List<ConceptModel> findAll() {
        return this.conceptRepository.findAll();
    }

    public void deleteAll() {
        this.conceptRepository.deleteAll();
    }

    public void setDataSource(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Transactional
    public String updateConceptsFromCSV(Long datasetId, List<String[]> concepts, Map<String, Integer> headerMap, List<String> metaColumnNames, int batch_size) {
        int varcount = concepts.size();
        log.debug("varcount: {}", varcount);
        int conceptUpdateCount = 0;
        int metaUpdateCount = 0;
        List<ConceptModel> conceptModels = new ArrayList<>();
        // map of concept path -> key/value map
        Map<String, Map<String, String>> metaMap = new HashMap<>();
        Map<String, String> parentMap = new HashMap<>();
        for (int i = 0; i < varcount; i++) {
            String[] var = concepts.get(i);
            if (var.length < headerMap.size()) {
                continue;
            }
            String conceptType = var[headerMap.get("concept_type")];
            String conceptPath = var[headerMap.get("concept_path")].replaceAll("'",
                    "''");
            String name = var[headerMap.get("name")];
            if (name.isEmpty()) {
                name = List.of(conceptPath.split("\\\\")).getLast();
            }
            String display = var[headerMap.get("display")];
            if (display.isEmpty()) {
                display = name;
            }
            String parentConceptPath = var[headerMap.get("parent_concept_path")].replaceAll("'",
                    "''");
            if (!parentConceptPath.isEmpty()) {
                parentMap.put(conceptPath, parentConceptPath);
            }
            ConceptModel newConceptModel;
            newConceptModel = new ConceptModel(conceptPath);
            newConceptModel.setConceptType(conceptType);
            newConceptModel.setDatasetId(datasetId);
            newConceptModel.setDisplay(display);
            newConceptModel.setName(name);
            conceptModels.add(newConceptModel);
            Map<String, String> metaVals = new HashMap<>();
            for (int j = 0; j < metaColumnNames.size(); j++) {
                String key = metaColumnNames.get(j);
                String value = var[headerMap.get(key)];
                if (!value.isBlank()) {
                    metaVals.put(key, value);
                }
            }
            metaMap.put(conceptPath, metaVals);

            if ((i % batch_size == 0 && i != 0) || i == varcount - 1) {

                // bulk update concept_node
                Query conceptQuery = entityManager.createNativeQuery(getUpsertConceptBatchQuery(conceptModels));

                conceptUpdateCount += conceptQuery.executeUpdate();

                // fetch updated concept node ids corresponding to concept paths
                Map<String, Long> metaConceptIdMap = new HashMap<>();

                List<Object[]> metaRefList = entityManager
                        .createNativeQuery(getIdsFromPathsQuery(metaMap.keySet())).getResultList();
                metaRefList.forEach(entry -> {
                    String path = escapeQuotesForSql(entry[0].toString());
                    Long conceptId = Long.parseLong(entry[1].toString());
                    metaConceptIdMap.put(path, conceptId);
                });

                // bulk update to add parent ids
                Map<String, Long> childConceptIdMap = new HashMap<>();
                List<Object[]> childConceptRefList = entityManager
                        .createNativeQuery(getIdsFromPathsQuery(parentMap.keySet())).getResultList();
                childConceptRefList.forEach(entry -> {
                    String path = escapeQuotesForSql(entry[0].toString());
                    Long conceptId = Long.parseLong(entry[1].toString());
                    childConceptIdMap.put(path, conceptId);
                });


                try {
                    Map<Long, String> idParentMap = parentMap.entrySet().stream()
                            .collect(
                                    Collectors.toMap(e -> (childConceptIdMap.get(e.getKey())),
                                            e -> (e.getValue())));
                    Query parentQuery = entityManager.createNativeQuery(getUpdateParentIdsQuery(idParentMap));
                    parentQuery.executeUpdate();
                } catch (NullPointerException e) {
                    log.error(e.getMessage(), e);

                }

                // bulk update concept_node_meta
                Map<Long, Map<String, String>> idMetaMap = metaMap.entrySet().stream()
                        .collect(Collectors.toMap(e -> (metaConceptIdMap.get(e.getKey())), e -> (e.getValue())));

                List<ConceptMetadataModel> metaList = new ArrayList<ConceptMetadataModel>();
                idMetaMap.entrySet().forEach(entry -> {
                    Long id = entry.getKey();
                    Map<String, String> metaEntries = entry.getValue();
                    metaEntries.keySet().forEach(metaKey -> {
                        ConceptMetadataModel conceptMeta = new ConceptMetadataModel();
                        conceptMeta.setConceptNodeId(id);
                        conceptMeta.setKey(metaKey);
                        conceptMeta.setValue(metaEntries.get(metaKey).toString());
                        metaList.add(conceptMeta);
                    });
                });
                Query metaQuery = entityManager.createNativeQuery(getUpsertConceptMetaBatchQuery(metaList));
                metaUpdateCount += metaQuery.executeUpdate();

                // clear all dataobjects for next batch
                conceptModels = new ArrayList<>();
                parentMap.clear();
                metaMap.clear();
                entityManager.flush();
            }
        }
        return "Successfully updated " + conceptUpdateCount + " concepts and " + metaUpdateCount
               + " concept meta entries from CSV. \n";
    }

    public String getUpsertConceptBatchQuery(List<ConceptModel> conceptModels) {
        log.info("List length: {}", conceptModels.size());
        String conceptPaths = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptModels.stream()
                        .map(model -> StringUtils.quote(model.getConceptPath()))
                        .collect(Collectors.toList()))
                              + "])";
        String conceptTypes = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptModels.stream().map(model -> StringUtils.quote(model.getConceptType()))
                        .collect(Collectors.toList()))
                              + "])";
        String datasetIds = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptModels.stream().map(model -> model.getDatasetId()).collect(Collectors.toList()))
                            + "])";
        String displays = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptModels.stream().map(model -> StringUtils.quote(model.getDisplay()))
                        .collect(Collectors.toList()))
                          + "])";
        String names = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptModels.stream().map(model -> StringUtils.quote(model.getName()))
                        .collect(Collectors.toList()))
                       + "])";

        String vals = StringUtils.arrayToCommaDelimitedString(
                new String[]{conceptPaths, conceptTypes, datasetIds, displays, names});

        String upsertQuery = "insert into concept_node (concept_path,concept_type,dataset_id,display,name) "
                             + "VALUES (" + vals + ")"
                             + " ON CONFLICT (md5(CONCEPT_PATH)) DO UPDATE SET (dataset_id,display,name) = (EXCLUDED.dataset_id,EXCLUDED.display,EXCLUDED.name);";
        log.debug("UPSERT QUERY: {}", upsertQuery);
        return upsertQuery;
    }

    public String getIdsFromPathsQuery(Set<String> paths) {
        String pathClause = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(paths.stream()
                .map(path -> StringUtils.quote(path))
                .collect(Collectors.toList())) + "])";
        String getIdsQuery = "select concept_path, concept_node_id from concept_node where concept_path in (select "
                             + pathClause + ")";
        log.debug("Get ids query: {}", getIdsQuery);
        return getIdsQuery;
    }

    public String getUpsertConceptMetaBatchQuery(List<ConceptMetadataModel> conceptMetaModels) {

        String conceptNodeIds = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptMetaModels.stream().map(model -> model.getConceptNodeId())
                        .collect(Collectors.toList()))
                                + "])";
        String keys = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptMetaModels.stream().map(model -> StringUtils.quote(model.getKey()))
                        .collect(Collectors.toList()))
                      + "])";
        String values = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                conceptMetaModels.stream()
                        .map(model -> StringUtils.quote(model.getValue().replaceAll("'",
                                "''")))
                        .collect(Collectors.toList()))
                        + "])";

        String vals = StringUtils.arrayToCommaDelimitedString(
                new String[]{conceptNodeIds, keys, values});

        String upsertQuery = "insert into concept_node_meta (concept_node_id,key,value) "
                             + "VALUES (" + vals + ")"
                             + " ON CONFLICT (key, concept_node_id) DO UPDATE SET value = EXCLUDED.value;";
        log.debug(upsertQuery);

        return upsertQuery;
    }

    public String getUpdateParentIdsQuery(Map<Long, String> parentConceptMap) {
        String conceptNodeIds = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                parentConceptMap.keySet())
                                + "])";
        String parentPaths = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(
                parentConceptMap.values().stream().map(val -> StringUtils.quote(val))
                        .collect(Collectors
                                .toList()))
                             + "])";

        return "with parent_table (node_id, parent_path) as"
               + "(select " + conceptNodeIds + ", " + parentPaths + ")"
               + "update concept_node set parent_id = parent_ref.p_id from "
               + "(select node_id, concept_node_id as p_id from parent_table left join concept_node on parent_path = concept_path) as parent_ref"
               + " where concept_node_id = node_id;";
    }


    public List<ConceptModel> findByDatasetID(Long datasetId) {
        return this.conceptRepository.findByDatasetId(datasetId);
    }

    public String escapeQuotesForSql(String str) {
        //Sanitizes input to ensure that single quotes in strings, such as apostrophes, are properly escaped for sql
        return str.replaceAll("'", "''");
    }
}
