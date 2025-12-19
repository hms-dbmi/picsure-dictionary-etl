package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class StudyFilter {

    public boolean isAllowed(String conceptPath, Set<String> allowedStudies) {
        if (allowedStudies == null || allowedStudies.isEmpty()) {
            return true;
        }

        String root = rootSegment(conceptPath);
        if (root == null) {
            return false;
        }

        return allowedStudies.contains(root.toLowerCase());
    }

    protected String rootSegment(String conceptPath) {
        if (!StringUtils.isNotBlank(conceptPath)) {
            return null;
        }

        String[] segments = conceptPath.split("\\\\");
        if (segments.length > 1) {
            return segments[1];
        }

        return segments[0];
    }



}
