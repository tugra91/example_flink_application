package com.flink.application.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;
import com.flink.application.model.IbbWifi;

/***
 * @author M.Tugra Er
 *     10.10.2022
 */
public class RowToModelTransformation extends RichMapFunction<Row, IbbWifi> {

    private static final long serialVersionUID = -8528744550951089716L;

    @Override
    public IbbWifi map(Row row) throws Exception {
        return new IbbWifi((Integer)row.getField(0),
                (String) row.getField(1),
                (String) row.getField(2),
                (String) row.getField(3),
                (String) row.getField(4),
                (String) row.getField(5),
                (String) row.getField(6),
                (Integer) row.getField(7)
                );
    }
}
