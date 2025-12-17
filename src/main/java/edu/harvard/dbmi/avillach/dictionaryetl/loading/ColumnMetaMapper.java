package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Component
public class ColumnMetaMapper {

    private static final String NULL = "null";

    public ColumnMeta mapCSVRowToColumnMeta(String[] columns) throws ArrayIndexOutOfBoundsException
    {
        boolean isCategorical = columns[3].equalsIgnoreCase("true");
        List<String> categoryValues = parseCategoryValuesToList(columns[4]);

        String conceptPath = getConceptPath(columns, isCategorical, categoryValues);

        Double min = null;
        Double max = null;
        if (!isCategorical) {
            if (StringUtils.hasLength(columns[5]) && !NULL.equals(columns[5])) {
                min = Double.parseDouble(columns[5]);
            }
            if (StringUtils.hasLength(columns[6]) && !NULL.equals(columns[6])) {
                max = Double.parseDouble(columns[6]);
            }
        }

        String col9 = getOptional(columns, 9);
        String col10 = getOptional(columns, 10);

        return new ColumnMeta(
                conceptPath,
                columns[1],
                columns[2],
                isCategorical,
                categoryValues,
                min,
                max,
                columns[7],
                columns[8],
                col9,
                col10
        );
    }

    private static String getConceptPath(String[] columns, boolean isCategorical, List<String> categoryValues) {
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

        return conceptPath;
    }

    private List<String> parseCategoryValuesToList(String column) {
        if (!StringUtils.hasLength(column)) {
            return new ArrayList<>();
        }

        // µ is used as our delimiter between categorical values.
        String[] categoricalValues = column.split("µ");
        return List.of(categoricalValues);
    }

    private static String getOptional(String[] columns, int idx) {
        if (idx >= columns.length) {
            return null;
        }

        String v = columns[idx];
        if (!StringUtils.hasLength(v) || NULL.equalsIgnoreCase(v)) {
            return null;
        }

        return v;
    }

}
