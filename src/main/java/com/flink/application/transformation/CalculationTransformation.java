package com.flink.application.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import com.flink.application.model.IbbWifi;

/***
 * @author M.Tugra Er
 *     10.10.2022
 */
public class CalculationTransformation extends RichMapFunction<IbbWifi, IbbWifi> {

    private static final long serialVersionUID = -4148254428395209592L;

    private ReducingStateDescriptor<Integer> totalUserStateDesc = null;
    private ReducingState<Integer> totalUserState = null;


    @Override
    public void open(Configuration configuration) {
        totalUserStateDesc = new ReducingStateDescriptor<>("total-user-state", Integer::sum, TypeInformation.of(Integer.class));
        totalUserState = getRuntimeContext().getReducingState(totalUserStateDesc);
    }

    @Override
    public IbbWifi map(IbbWifi ibbWifi) throws Exception {
        totalUserState.add(ibbWifi.getUserCount());
        ibbWifi.setTotalUserCount(totalUserState.get());
        return ibbWifi;
    }
}
