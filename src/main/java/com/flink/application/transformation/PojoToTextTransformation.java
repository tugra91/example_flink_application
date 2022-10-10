package com.flink.application.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import com.flink.application.model.IbbWifi;

/***
 * @author M.Tugra Er
 *     10.10.2022
 */
public class PojoToTextTransformation extends RichMapFunction<IbbWifi, String> {

    private static final long serialVersionUID = -7609181805295981276L;

    @Override
    public String map(IbbWifi ibbWifi) throws Exception {
        return ibbWifi.toString();
    }
}
