package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetCategoryDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetCategoryWrapper;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.Result;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacetLoaderConceptMetaDrivenIntegrationTest {

    @Autowired private DatabaseCleanupUtility databaseCleanupUtility;
    @Autowired private FacetLoaderService service;
    @Autowired private DatasetRepository datasetRepository;
    @Autowired private ConceptService conceptService;
    @Autowired private ConceptMetadataRepository conceptMetadataRepository;
    @Autowired private FacetRepository facetRepository;
    @Autowired private FacetConceptRepository facetConceptRepository;

    @Container
    static final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>("postgres:16")
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("schema.sql"),
            "/docker-entrypoint-initdb.d/schema.sql"
        );

    @DynamicPropertySource
    static void mySQLProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", databaseContainer::getJdbcUrl);
        registry.add("spring.datasource.username", databaseContainer::getUsername);
        registry.add("spring.datasource.password", databaseContainer::getPassword);
    }

    @BeforeEach
    void cleanDatabase() {
        databaseCleanupUtility.truncateTablesAllTables();
    }

    /** Builds the metadata-driven category using the category-level Concept_Meta_Key. */
    private static FacetCategoryWrapper metadataCategory(String name, String display, String metaKey) {
        FacetCategoryDTO cat = new FacetCategoryDTO(name, display, null, null, List.of(), metaKey);
        return new FacetCategoryWrapper(cat);
    }

    @Test
    void generatorSpec_shouldCreateOneFacetPerDistinctMetaValue() {
        DatasetModel ds = datasetRepository.save(new DatasetModel("phs001", "Study 1", "", ""));

        ConceptModel c1 = conceptService.save(new ConceptModel(ds.getDatasetId(), "c1", "c1", "", "\\phs001\\c1\\", null));
        ConceptModel c2 = conceptService.save(new ConceptModel(ds.getDatasetId(), "c2", "c2", "", "\\phs001\\c2\\", null));
        ConceptModel c3 = conceptService.save(new ConceptModel(ds.getDatasetId(), "c3", "c3", "", "\\phs001\\c3\\", null));

        conceptMetadataRepository.upsert(c1.getConceptNodeId(), "subject_type", "Human");
        conceptMetadataRepository.upsert(c2.getConceptNodeId(), "subject_type", "Mouse");
        conceptMetadataRepository.upsert(c3.getConceptNodeId(), "subject_type", "Human");

        Result result = service.load(List.of(metadataCategory("subject_type", "Subject Type", "subject_type")));

        assertEquals(1, result.categoriesCreated());
        assertEquals(2, result.facetsCreated()); // Human + Mouse

        Optional<FacetModel> humanFacet = facetRepository.findByName("Human");
        Optional<FacetModel> mouseFacet = facetRepository.findByName("Mouse");
        assertTrue(humanFacet.isPresent());
        assertTrue(mouseFacet.isPresent());

        Long humanId = humanFacet.get().getFacetId();
        Long mouseId = mouseFacet.get().getFacetId();

        // Human: c1 and c3 mapped, not c2
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(humanId, c1.getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(humanId, c3.getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(humanId, c2.getConceptNodeId()).isEmpty());

        // Mouse: c2 mapped only
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(mouseId, c2.getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(mouseId, c1.getConceptNodeId()).isEmpty());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(mouseId, c3.getConceptNodeId()).isEmpty());
    }

    @Test
    void generatorSpec_reloadIsIdempotent() {
        DatasetModel ds = datasetRepository.save(new DatasetModel("phs002", "Study 2", "", ""));
        ConceptModel c1 = conceptService.save(new ConceptModel(ds.getDatasetId(), "c1", "c1", "", "\\phs002\\c1\\", null));
        conceptMetadataRepository.upsert(c1.getConceptNodeId(), "subject_type", "Human");

        FacetCategoryWrapper wrapper = metadataCategory("subject_type", "Subject Type", "subject_type");
        service.load(List.of(wrapper));
        Result second = service.load(List.of(wrapper));

        assertEquals(0, second.categoriesCreated());
        assertEquals(0, second.facetsCreated());

        assertEquals(1, facetRepository.findAll().size());
        Optional<FacetModel> humanFacet = facetRepository.findByName("Human");
        assertTrue(humanFacet.isPresent());
        assertEquals(1, facetConceptRepository.countForFacet(humanFacet.get().getFacetId()));
    }

    @Test
    void generatorSpec_conceptWithNoMetadataIsNotMapped() {
        DatasetModel ds = datasetRepository.save(new DatasetModel("phs003", "Study 3", "", ""));
        ConceptModel c1 = conceptService.save(new ConceptModel(ds.getDatasetId(), "c1", "c1", "", "\\phs003\\c1\\", null));
        ConceptModel c2 = conceptService.save(new ConceptModel(ds.getDatasetId(), "c2", "c2", "", "\\phs003\\c2\\", null));

        conceptMetadataRepository.upsert(c1.getConceptNodeId(), "subject_type", "Human");
        // c2 has no subject_type metadata

        service.load(List.of(metadataCategory("subject_type", "Subject Type", "subject_type")));

        Optional<FacetModel> humanFacet = facetRepository.findByName("Human");
        assertTrue(humanFacet.isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(humanFacet.get().getFacetId(), c1.getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(humanFacet.get().getFacetId(), c2.getConceptNodeId()).isEmpty());
    }
}
