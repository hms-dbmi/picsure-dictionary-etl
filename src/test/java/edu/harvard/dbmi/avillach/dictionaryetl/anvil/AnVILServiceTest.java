package edu.harvard.dbmi.avillach.dictionaryetl.anvil;

import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentModel;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

@SpringBootTest
@ContextConfiguration(classes = {
        AnVILService.class
})
public class AnVILServiceTest {

    @Autowired
    private AnVILService anVILService;

    @MockBean
    private DatasetRepository datasetRepository;

    @MockBean
    private ConsentRepository consentRepository;

    @MockBean
    private DatasetMetadataRepository datasetMetadataRepository;

    private static String fileContents;

    @BeforeAll
    public static void init() throws IOException {
        ClassPathResource anvilStudiesDataset = new ClassPathResource("AnVIL Studies Sample.tsv");
        assertNotNull(anvilStudiesDataset);

        // Read in file contents
        fileContents = Files.readString(anvilStudiesDataset.getFile().toPath());
    }

    @Test
    public void testSerializedData() throws IOException {
        assertNotNull(fileContents);
        List<AnVILStudyMetadata> anVILStudyMetadataList = this.anVILService.serializeData(fileContents);
        assertFalse(anVILStudyMetadataList.isEmpty());
    }

    @Test
    public void testFindExistingRefs() {
        // We are going to say only the following studies don't exist in the db.
        List<String> newRefs = List.of("phs001746");
        Mockito.when(datasetRepository.findValuesNotInRef(any())).thenReturn(newRefs);

        List<AnVILStudyMetadata> anVILStudyMetadata = this.anVILService.serializeData(fileContents);
        assertFalse(anVILStudyMetadata.isEmpty());

        // mock the database request
        List<String> existingRefs = this.anVILService.findExistingRefs(anVILStudyMetadata);
        assertFalse(existingRefs.isEmpty());
        String testPhsVal = existingRefs.get(0);
        assertNotNull(testPhsVal);
        assertEquals(testPhsVal, "phs001746");
    }

    @Test
    public void testIngestAnVILData() {
        Mockito.when(datasetRepository.findValuesNotInRef(any())).thenReturn(List.of("phs001746", "phs001798"));
        DatasetModel datasetModel = new DatasetModel();
        datasetModel.setDatasetId(1L);
        Mockito.when(datasetRepository.save(any())).thenReturn(datasetModel);
        Mockito.when(consentRepository.save(any(ConsentModel.class))).thenReturn(new ConsentModel());
        Mockito.when(datasetMetadataRepository.saveAll(anyList())).thenReturn(List.of());

        List<AnVILStudyMetadata> anVILStudyMetadata = this.anVILService.ingestAnVILData(fileContents);
        assertFalse(anVILStudyMetadata.isEmpty());
        assertEquals(2, anVILStudyMetadata.size());
    }

}