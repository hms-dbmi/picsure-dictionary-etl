package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

@Service
public class ConceptService {
        private JdbcTemplate jdbcTemplate;

        private final ConceptRepository conceptRepository;

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
                this.jdbcTemplate = new JdbcTemplate(dataSource);
        }

        public String getUpsertConceptBatchQuery(List<ConceptModel> conceptModels) {
                 System.out.println("List length: " + conceptModels.size());
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
                                new String[] { conceptPaths, conceptTypes, datasetIds, displays, names });

                String upsertQuery = "insert into concept_node (concept_path,concept_type,dataset_id,display,name) "
                                + "VALUES (" + vals + ")"
                                + " ON CONFLICT (md5(CONCEPT_PATH)) DO UPDATE SET (dataset_id,display,name) = (EXCLUDED.dataset_id,EXCLUDED.display,EXCLUDED.name);";
                // System.out.println("UPSERT QUERY: " + upsertQuery);
                return upsertQuery;
        }

        public String getIdsFromPathsQuery(Set<String> paths) {
                String pathClause = "UNNEST(ARRAY[" + StringUtils.collectionToCommaDelimitedString(paths.stream()
                                .map(path -> StringUtils.quote(path))
                                .collect(Collectors.toList())) + "])";
                String getIdsQuery = "select concept_path, concept_node_id from concept_node where concept_path in (select "
                                + pathClause + ")";
                // System.out.println("Get ids query: " + getIdsQuery);
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
                                new String[] { conceptNodeIds, keys, values });

                String upsertQuery = "insert into concept_node_meta (concept_node_id,key,value) "
                                + "VALUES (" + vals + ")"
                                + " ON CONFLICT (key, concept_node_id) DO UPDATE SET value = EXCLUDED.value;";
                //System.out.println(upsertQuery);

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

                String updateQuery = "with parent_table (node_id, parent_path) as"
                                + "(select " + conceptNodeIds + ", " + parentPaths + ")"
                                + "update concept_node set parent_id = parent_ref.p_id from "
                                + "(select node_id, concept_node_id as p_id from parent_table left join concept_node on parent_path = concept_path) as parent_ref"
                                + " where concept_node_id = node_id;";

                return updateQuery;
        }
        //TODO generalize this to use in all classes(general utility class) Currently duplicating in multiple services
        public Map<String, Integer> buildCsvInputsHeaderMap(String[] headers) {
                Map<String, Integer> inputsHeaders = new HashMap<String, Integer>();
                for (int i = 0; i < headers.length; i++) {
                        inputsHeaders.put(headers[i], i);
                }
                return inputsHeaders;
        }

        public List<ConceptModel> findByDatasetID(Long datasetId) {
                return this.conceptRepository.findByDatasetId(datasetId);
        }
}
