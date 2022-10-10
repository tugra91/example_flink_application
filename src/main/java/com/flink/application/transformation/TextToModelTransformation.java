package com.flink.application.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import com.flink.application.model.IbbWifi;

/***
 * @author M.Tugra Er
 *     10.10.2022
 */
public class TextToModelTransformation extends RichMapFunction<String, IbbWifi> {
    private static final long serialVersionUID = 1573828535941385601L;

    @Override
    public IbbWifi map(String s) throws Exception {
        String[] rowArr = s.split(",");
        return rowArr.length == 8 ? new IbbWifi(Integer.valueOf(rowArr[0]),
                    rowArr[1],
                    rowArr[2],
                    rowArr[3],
                    rowArr[4],
                    rowArr[5],
                    rowArr[6],
                    Integer.valueOf(rowArr[7])) : new IbbWifi(null, null, null, null, null, null, null, null);
    }
}
