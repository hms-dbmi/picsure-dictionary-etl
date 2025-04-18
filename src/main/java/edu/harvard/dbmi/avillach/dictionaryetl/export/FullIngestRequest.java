package edu.harvard.dbmi.avillach.dictionaryetl.export;

import java.util.List;

public record FullIngestRequest(String downloadPath, List<String> phsIds) {
}
