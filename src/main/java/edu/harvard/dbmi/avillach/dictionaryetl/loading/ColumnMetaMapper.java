package edu.harvard.dbmi.avillach.dictionaryetl.loading;

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
            new CSVParserBuilder().withSeparator(',').withQuoteChar('"').withEscapeChar(CSVParser.NULL_CHARACTER).build();
    private final Logger log = LoggerFactory.getLogger(ColumnMetaMapper.class);
    private static final String NULL = "null";

    public Optional<ColumnMeta> mapCSVRowToColumnMeta(String csvRow) {
        try {
            String[] columns = parser.parseLine(csvRow);
            boolean isCategorical = columns[3].charAt(0) == 't';
            List<String> categoryValues = parseCategoryValuesToList(columns[4]);

            String conceptPath = columns[0].replace("µ", "\\");
            if (isCategorical && categoryValues.size() == 1) {
                int lastBackslash = conceptPath.lastIndexOf("\\");
                int secondLastBackslash = conceptPath.lastIndexOf("\\", lastBackslash - 1);
                String lastNode = conceptPath.substring(secondLastBackslash + 1).replace("\\", "");
                if (lastNode.equals(categoryValues.getFirst())) {
                    // remove the last node from the concept path
                    conceptPath = conceptPath.substring(0, secondLastBackslash + 1);
                }
            }

            double min = 0;
            double max = 0;
            if (!isCategorical) {
                if (StringUtils.hasLength(columns[5]) && !NULL.equals(columns[5])) {
                    min = Double.parseDouble(columns[5]);
                }
                if (StringUtils.hasLength(columns[6]) && !NULL.equals(columns[6])) {
                    max = Double.parseDouble(columns[6]);
                }
            }

            return Optional.of(new ColumnMeta(
                    conceptPath,
                    columns[1],
                    columns[2],
                    isCategorical,
                    categoryValues,
                    min,
                    max,
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
