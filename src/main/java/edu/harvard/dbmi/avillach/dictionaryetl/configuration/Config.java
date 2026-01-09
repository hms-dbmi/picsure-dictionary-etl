package edu.harvard.dbmi.avillach.dictionaryetl.configuration;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    @Bean
    public CSVParser csvParser() {
        return new CSVParserBuilder().withSeparator(',').withQuoteChar('"').withEscapeChar(CSVParser.NULL_CHARACTER).build();
    }

}
