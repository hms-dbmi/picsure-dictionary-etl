package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = StudyFilter.class)
class StudyFilterTest {

    @Autowired
    StudyFilter studyFilter;

    @Test
    void shouldReturnRootSegment() {
        String rootSegment = this.studyFilter.rootSegment("\\test\\concept\\path\\");
        assertEquals("test", rootSegment);
    }

    @Test
    void isAllowed() {
        String conceptPath = "\\test\\concept\\path\\";
        assertTrue(this.studyFilter.isAllowed(conceptPath, Set.of("test")));
    }

    @Test
    void isNotAllowed() {
        String conceptPath = "\\test\\concept\\path\\";
        assertFalse(this.studyFilter.isAllowed(conceptPath, Set.of("notAllowed")));
    }

    @Test
    void isEmptyAllowed() {
        String conceptPath = "\\test\\concept\\path\\";
        assertTrue(this.studyFilter.isAllowed(conceptPath, Set.of()));
    }

}