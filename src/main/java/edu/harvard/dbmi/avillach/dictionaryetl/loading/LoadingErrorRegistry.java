package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LoadingErrorRegistry {

    private final Set<String> errors = ConcurrentHashMap.newKeySet();

    public void addError(String error) {
        errors.add(error);
    }

    public Set<String> getErrors() {
        return errors;
    }

    public void clear() {
        errors.clear();
    }

}
