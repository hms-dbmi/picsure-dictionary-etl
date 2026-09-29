package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.micrometer.common.util.StringUtils;

import java.util.List;

/**
 * FHIR R4 CodeableConcept datatype (subset). Used by ResearchStudy.category and ResearchStudy.focus.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CodeableConcept(List<Coding> coding, String text) {

    /**
     * Human-readable value: {@code text} when present, otherwise the first non-blank {@code coding[].display}. Not a bean getter on purpose
     * so Jackson does not serialize it.
     *
     * @return the display value, or null when neither text nor a coding display is available
     */
    public String displayValue() {
        if (StringUtils.isNotBlank(text)) {
            return text.trim();
        }
        if (coding == null) {
            return null;
        }
        return coding.stream().filter(c -> c != null && StringUtils.isNotBlank(c.display())).map(c -> c.display().trim()).findFirst()
            .orElse(null);
    }
}
