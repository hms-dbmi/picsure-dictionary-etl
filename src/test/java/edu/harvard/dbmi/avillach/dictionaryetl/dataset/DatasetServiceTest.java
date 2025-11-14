package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatasetServiceTest {

    @Mock
    private DatasetRepository datasetRepository;

    @InjectMocks
    private DatasetService datasetService;

    @Test
    void deleteByRef_shouldDelegateToRepository_andReturnDeletedCount_nonZero() {
        String ref = "TEST_REF";
        when(datasetRepository.deleteByRef(ref)).thenReturn(1);

        int deleted = datasetService.deleteByRef(ref);

        assertEquals(1, deleted);
        verify(datasetRepository, times(1)).deleteByRef(ref);
        verifyNoMoreInteractions(datasetRepository);
    }

    @Test
    void deleteByRef_shouldDelegateToRepository_andReturnDeletedCount_zero() {
        String ref = "MISSING_REF";
        when(datasetRepository.deleteByRef(ref)).thenReturn(0);

        int deleted = datasetService.deleteByRef(ref);

        assertEquals(0, deleted);
        verify(datasetRepository, times(1)).deleteByRef(ref);
        verifyNoMoreInteractions(datasetRepository);
    }
}
