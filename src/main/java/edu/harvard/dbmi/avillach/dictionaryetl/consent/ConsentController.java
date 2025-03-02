package edu.harvard.dbmi.avillach.dictionaryetl.consent;

import static org.mockito.Mockito.description;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class ConsentController {
    @Autowired
    ConsentRepository consentRepository;

    @Autowired
    DatasetRepository datasetRepository;

    @GetMapping("/consent")
    public ResponseEntity<List<ConsentModel>> getAllConsentModels(@RequestParam(required = false) String datasetRef) {
        try {
            List<ConsentModel> consentModels = new ArrayList<ConsentModel>();

            if (datasetRef == null) {
                // get all consents in dictionary
                consentRepository.findAll().forEach(consentModels::add);
            } else {
                // get all consents in specific dataset
                Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
                consentRepository.findByDatasetId(datasetId).forEach(consentModels::add);

            }
            if (consentModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(consentModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/consent")
    public ResponseEntity<ConsentModel> updateConsent(@RequestParam String datasetRef, @RequestParam String consentCode,
            @RequestParam String description, @RequestParam String authz, @RequestParam Long participantCount,
            @RequestParam Long variableCount, @RequestParam Long sampleCount) {
        Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
        Optional<ConsentModel> consentData = consentRepository.findByConsentCodeAndDatasetId(consentCode, datasetId);

        if (consentData.isPresent()) {
            // update already existing consent
            ConsentModel existingConsent = consentData.get();
            existingConsent.setDatasetId(datasetId);
            existingConsent.setConsentCode(consentCode);
            existingConsent.setDescription(description);
            existingConsent.setAuthz(authz);
            existingConsent.setParticipantCount(participantCount);
            existingConsent.setVariableCount(variableCount);
            existingConsent.setSampleCount(sampleCount);
            return new ResponseEntity<>(consentRepository.save(existingConsent), HttpStatus.OK);
        } else {
            // add new consent when consent not present in data
            try {
                ConsentModel newConsent = consentRepository
                        .save(new ConsentModel(datasetId, consentCode,
                                description, authz, participantCount,
                                variableCount, sampleCount));
                return new ResponseEntity<>(newConsent, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @PutMapping("/consent/csv")
    public ResponseEntity<Object> updateConsentCsv(@RequestParam String datasetRef, @RequestBody String input) {
        DatasetService service = new DatasetService(datasetRepository);
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        List<String[]> consents = new ArrayList<>();
        Map<String, Integer> headerMap = new HashMap<String, Integer>();
        try (CSVReader reader = new CSVReader(new StringReader(input))) {
            String[] header = reader.readNext();
            headerMap = service.buildCsvInputsHeaderMap(header);
            String[] coreConsentHeaders = { "datasetRef", "consentCode", "description", "participantCount",
                    "variableCount",
                    "sampleCount", "authz" };
            if (!headerMap.keySet().containsAll(Arrays.asList(coreConsentHeaders))) {
                return new ResponseEntity<>(
                        "Headers in Consent ingest file incorrect for " + datasetRef,
                        HttpStatus.BAD_REQUEST);
            }
            consents = reader.readAll();
            consents.remove(header);
            reader.close();
        } catch (IOException | CsvException e) {
            return new ResponseEntity<>(
                    "Error reading consent ingestion csv for " + datasetRef + ". Error: \n" + e.getStackTrace(),
                    HttpStatus.BAD_REQUEST);
        }
        if (consents.isEmpty()) {
            return new ResponseEntity<>(
                    "No csv records found in input file.",
                    HttpStatus.BAD_REQUEST);
        }

        int consentCount = consents.size();
        int consentUpdateCount = 0;

        for (int i = 0; i < consentCount; i++) {
            String[] consent = consents.get(i);
            if (consent.length < headerMap.size())
                continue;
            String consentCode = consent[headerMap.get("consentCode")];
            String description = consent[headerMap.get("description")];
            Long participantCount;
            Long variableCount;
            Long sampleCount;
            try {
                participantCount = Long.parseLong(consent[headerMap.get("participantCount")]);
            } catch (NumberFormatException e) {
                participantCount = Long.parseLong("-1");
            }
            try {
                variableCount = Long.parseLong(consent[headerMap.get("variableCount")]);
            } catch (NumberFormatException e) {
                variableCount = Long.parseLong("-1");
            }
            try {
                sampleCount = Long.parseLong(consent[headerMap.get("sampleCount")]);
            } catch (NumberFormatException e) {
                sampleCount = Long.parseLong("-1");
            }
            String authz = consent[headerMap.get("authz")];
            Optional<ConsentModel> consentData = consentRepository.findByConsentCodeAndDatasetId(consentCode,
                    datasetId);
            ConsentModel consentModel;
            if (consentData.isPresent()) {
                // update already existing consent
                consentModel = consentData.get();
                consentModel.setDatasetId(datasetId);
                consentModel.setConsentCode(consentCode);
                consentModel.setDescription(description);
                consentModel.setAuthz(authz);
                consentModel.setParticipantCount(participantCount);
                consentModel.setVariableCount(variableCount);
                consentModel.setSampleCount(sampleCount);
            } else {
                // add new consent when consent not present in data
                consentModel = new ConsentModel(datasetId, consentCode,
                        description, authz, participantCount,
                        variableCount, sampleCount);

            }
            consentRepository.save(consentModel);
            consentUpdateCount++;
        }

        return new ResponseEntity<>(
                "Successfully created/updated " + consentUpdateCount + " consents for " + datasetRef, HttpStatus.OK);

    }

    @PutMapping("/consent/counts")
    public ResponseEntity<ConsentModel> updateConsentCounts(@RequestParam String datasetRef,
            @RequestParam String consentCode,
            @RequestParam Optional<Long> participantCount, @RequestParam Optional<Long> variableCount,
            @RequestParam Optional<Long> sampleCount) {
        Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
        Optional<ConsentModel> consentData = consentRepository.findByConsentCodeAndDatasetId(consentCode, datasetId);

        if (consentData.isPresent()) {
            // update already existing consent
            ConsentModel existingConsent = consentData.get();
            if (participantCount.isPresent())
                existingConsent.setParticipantCount(participantCount.get());
            if (variableCount.isPresent())
                existingConsent.setVariableCount(variableCount.get());
            if (sampleCount.isPresent())
                existingConsent.setSampleCount(sampleCount.get());

            return new ResponseEntity<>(consentRepository.save(existingConsent), HttpStatus.OK);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @DeleteMapping("/consent")
    public ResponseEntity<ConsentModel> deleteConsent(@RequestParam String consentCode,
            @RequestParam String datasetRef) {

        Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
        Optional<ConsentModel> consentData = consentRepository.findByConsentCodeAndDatasetId(consentCode, datasetId);

        if (consentData.isPresent()) {
            consentRepository.delete(consentData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            // add new consent when consent not present in data
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
