package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.micrometer.common.util.StringUtils;

/**
 * FHIR R4 Reference datatype (subset). Used by ResearchStudy.sponsor.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Reference(String reference, String display) {

    /**
     * Human-readable value of the reference ({@code display}). Not a bean getter on purpose so Jackson does not serialize it.
     *
     * @return the trimmed display, or null when blank
     */
    public String displayValue() {
        return StringUtils.isNotBlank(display) ? display.trim() : null;
    }
}
