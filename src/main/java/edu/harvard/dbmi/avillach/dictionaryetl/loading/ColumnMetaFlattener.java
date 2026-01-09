package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class ColumnMetaFlattener {

    public ColumnMeta flatten(List<ColumnMeta> columnMetas) {
        ColumnMeta columnMeta;
        if (columnMetas.size() == 1) {
            columnMeta = columnMetas.getFirst();
        } else {
            boolean isContinuous = columnMetas.stream().anyMatch(meta -> !meta.categorical());
            if (!isContinuous) {
                columnMeta = flattenCategoricalColumnMeta(columnMetas);
            } else {
                columnMeta = flattenContinuousColumnMeta(columnMetas);
            }
        }

        return columnMeta;
    }

    private ColumnMeta flattenContinuousColumnMeta(List<ColumnMeta> columnMetas) throws IllegalArgumentException {
        if (columnMetas.getFirst().categorical()) {
            throw new IllegalArgumentException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                               "for a concept path. ColumnMetas: " + StringUtils.joinWith(",", columnMetas));
        }

        final Double[] min = {columnMetas.getFirst().min()};
        final Double[] max = {columnMetas.getFirst().max()};

        for (ColumnMeta columnMeta : columnMetas) {
            if (columnMeta.categorical()) {
                if (columnMeta.categoryValues().size() > 1) {
                    throw new IllegalArgumentException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                                       "for a concept path. ColumnMetas: " + StringUtils.joinWith(",", columnMetas));
                }

                double value = Double.parseDouble(columnMeta.categoryValues().getFirst());
                min[0] = Math.min(min[0], value);
                max[0] = Math.max(max[0], value);
            } else {
                min[0] = Math.min(min[0], columnMeta.min());
                max[0] = Math.max(max[0], columnMeta.max());
            }
        }

        return new ColumnMeta(
                columnMetas.getFirst().name(),
                null,
                null,
                false,
                columnMetas.getFirst().categoryValues(),
                min[0],
                max[0],
                null,
                null,
                null,
                null
        );
    }

    private ColumnMeta flattenCategoricalColumnMeta(List<ColumnMeta> columnMetas) {
        Set<String> setOfVals = new HashSet<>();
        columnMetas.forEach(columnMeta -> setOfVals.addAll(columnMeta.categoryValues()));

        List<String> values = new ArrayList<>(setOfVals);
        return new ColumnMeta(
                columnMetas.getFirst().name(),
                null,
                null,
                columnMetas.getFirst().categorical(),
                values,
                columnMetas.getFirst().min(),
                columnMetas.getFirst().max(),
                null,
                null,
                null,
                null
        );
    }

}
