package edu.harvard.dbmi.avillach.dictionaryetl.columnmeta;

import java.util.List;

public record ColumnMeta(
    String name,
    String widthInBytes,
    String columnOffset,
    boolean categorical,
    List<String> categoryValues,
    Double min,
    Double max,
    String allObservationsOffset,
    String allObservationsLength,
    String observationCount,
    String patientCount
) {
}
