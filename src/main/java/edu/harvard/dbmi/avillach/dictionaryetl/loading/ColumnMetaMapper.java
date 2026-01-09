package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Component
public class ColumnMetaMapper {

    private static final String NULL = "null";

    public ColumnMeta mapCSVRowToColumnMeta(String[] cells) throws ArrayIndexOutOfBoundsException {
        boolean isCategorical = cells[3].equalsIgnoreCase("true");
        List<String> categoryValues = parseCategoryValuesToList(cells[4]);

        String conceptPath = getConceptPath(cells, isCategorical, categoryValues);

        Double min = null;
        Double max = null;
        if (!isCategorical) {
            if (StringUtils.hasLength(cells[5]) && !NULL.equals(cells[5])) {
                min = Double.parseDouble(cells[5]);
            }
            if (StringUtils.hasLength(cells[6]) && !NULL.equals(cells[6])) {
                max = Double.parseDouble(cells[6]);
            }
        }

        String col9 = getOptional(cells, 9);
        String col10 = getOptional(cells, 10);

        return new ColumnMeta(
                conceptPath,
                cells[1],
                cells[2],
                isCategorical,
                categoryValues,
                min,
                max,
                cells[7],
                cells[8],
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
