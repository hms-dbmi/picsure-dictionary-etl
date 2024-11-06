package edu.harvard.dbmi.avillach.dictionaryetl.anvil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@Controller
@RequestMapping("/anvil")
public class AnVILController {

    private static final Logger log = LoggerFactory.getLogger(AnVILController.class);
    private final AnVILService anVILService;

    @Autowired
    public AnVILController(AnVILService anVILService) {
        this.anVILService = anVILService;
    }

    /**
     * Expects a AnVIL TSV file content. The TSV file can be found here:
     * <a href="https://anvilproject.org/data/studies">AnVIL Dataset Catalog</a>
     *
     * @param requestBody String containing the contents of the CSV
     * @return A List of the AnVIL study metadata
     */
    @PostMapping("/upload-tsv")
    public ResponseEntity<List<AnVILStudyMetadata>> uploadTsvFile(@RequestBody String requestBody) {
        log.debug("uploadTsvFile received data: {}", requestBody);
        return ResponseEntity.ok(this.anVILService.ingestAnVILData(requestBody));
    }

}
