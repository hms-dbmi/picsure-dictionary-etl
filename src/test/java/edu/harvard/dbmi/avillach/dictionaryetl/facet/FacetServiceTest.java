package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FacetServiceTest {

    @Mock
    private FacetRepository facetRepository;

    @Mock
    private FacetCategoryService facetCategoryService;

    @Mock
    private FacetConceptService facetConceptService;

    @Mock
    private FacetMetadataRepository facetMetadataRepository;

    @InjectMocks
    private FacetService facetService;

    @Test
    void deleteByName_shouldDelegateToRepository_andReturnDeletedCount_nonZero() {
        String name = "DATASET_REF";
        when(facetRepository.deleteByName(name)).thenReturn(2);

        int deleted = facetService.deleteByName(name);

        assertEquals(2, deleted);
        verify(facetRepository, times(1)).deleteByName(name);
        verifyNoMoreInteractions(facetRepository);
    }

    @Test
    void deleteByName_shouldDelegateToRepository_andReturnDeletedCount_zero() {
        String name = "MISSING";
        when(facetRepository.deleteByName(name)).thenReturn(0);

        int deleted = facetService.deleteByName(name);

        assertEquals(0, deleted);
        verify(facetRepository, times(1)).deleteByName(name);
        verifyNoMoreInteractions(facetRepository);
    }
}
