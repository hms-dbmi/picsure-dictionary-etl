package edu.harvard.dbmi.avillach.dictionaryetl.hydration;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class ColumnMetaMapper {

    private final CSVParser parser =
            new CSVParserBuilder().withSeparator(',').withQuoteChar('"').withEscapeChar('\0').build();
    private final Logger log = LoggerFactory.getLogger(ColumnMetaMapper.class);

    public Optional<ColumnMeta> mapCSVRowToColumnMeta(String csvRow) {
        String[] columns;
        try {
            columns = parser.parseLine(csvRow);
        List<String> categoryValues = parseCategoryValuesToList(columns[4]);
        boolean isCategorical = Boolean.parseBoolean(columns[3]);
        String conceptPath = columns[0];
        if (isCategorical && categoryValues.size() == 1) {
            // split the concept path on \\ so we can remove the trailing value.
            // Example concept path: \demographics\RACE\black\
            // "black" is actually the categorical value.
            conceptPath = conceptPath.replace(categoryValues.getFirst() + "\\", "");
        }

        return Optional.of(new ColumnMeta(
                conceptPath,
                columns[1],
                columns[2],
                isCategorical,
                categoryValues,
                !isCategorical && StringUtils.hasLength(columns[5]) && !columns[5].equals("null") ?
                        Double.parseDouble(columns[5]) : 0,
                !isCategorical && StringUtils.hasLength(columns[6]) && !columns[6].equals("null") ?
                        Double.parseDouble(columns[6]) : 0,
                columns[7],
                columns[8],
                columns[9],
                columns[10]
        ));

        } catch (IOException e) {
            log.error("Unable to parse line {}", csvRow);
        }

        return Optional.empty();
    }

    private List<String> parseCategoryValuesToList(String column) {
        if (!StringUtils.hasLength(column)) {
            return new ArrayList<>();
        }

        // µ is used as our delimiter between categorical values.
        String[] categoricalValues = column.split("µ");
        return List.of(categoricalValues);
    }

}
