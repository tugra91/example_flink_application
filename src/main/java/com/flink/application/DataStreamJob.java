/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.application;

import com.flink.application.transformation.PojoToTextTransformation;
import com.flink.application.transformation.RowToModelTransformation;
import com.flink.application.transformation.TextToModelTransformation;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.Row;
import com.flink.application.model.IbbWifi;
import com.flink.application.transformation.CalculationTransformation;
import com.flink.application.util.CommonUtil;

import java.nio.charset.StandardCharsets;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {


	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		CommonUtil commonUtil = new CommonUtil();

		Path csvFilePath = commonUtil.getFileFromResource("ibb_wifi_user_counts.csv");
		BasicTypeInfo[] typeInfoList = new BasicTypeInfo[]{BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO};
		RowCsvInputFormat rowCsvInputFormat = new RowCsvInputFormat(csvFilePath, typeInfoList , "\n", ",");
		DataStream<Row> dataStreamCsv =  env.readFile(rowCsvInputFormat, csvFilePath.getPath(), FileProcessingMode.PROCESS_ONCE, 10);

		DataStream<IbbWifi> modelStreamCsv = dataStreamCsv.map(new RowToModelTransformation());

		KeyedStream<IbbWifi, String>  keyedStreamCsv = modelStreamCsv.keyBy(IbbWifi::getSubscriberType);

		SingleOutputStreamOperator<IbbWifi> calculationStreamCsv = keyedStreamCsv.map(new CalculationTransformation());

		SingleOutputStreamOperator<String> modelToTextStreamCsv = calculationStreamCsv.map(new PojoToTextTransformation());

		/* You can change file path according to your file organization. But On my mac,
			I faced an issue that it didn't save it to location except for the project path.
			If you are using a Mac, please consider this case.  */
		StreamingFileSink<String> fileSink = StreamingFileSink
				.forRowFormat(new Path("/Users/tugra/Dev/gerimedica/deneme/ibb_wifi_user_count_csv_outputs"), new SimpleStringEncoder<String>())
				.build();

		modelToTextStreamCsv.addSink(fileSink);


		Path filePath = commonUtil.getFileFromResource("ibb_wifi_user_counts.txt");
		TextInputFormat textInputFormat = new TextInputFormat(filePath);
		textInputFormat.setFilesFilter(FilePathFilter.createDefaultFilter());
		textInputFormat.setCharsetName(StandardCharsets.UTF_8.name());
		DataStream<String> dataStreamText =  env.readFile(textInputFormat, filePath.getPath(), FileProcessingMode.PROCESS_ONCE, 10);

		DataStream<IbbWifi> modelStream = dataStreamText.map(new TextToModelTransformation());


		KeyedStream<IbbWifi, String> keyedStream = modelStream.keyBy(IbbWifi::getSubscriberType);

		SingleOutputStreamOperator<IbbWifi> calculateTotalVehicleCount = keyedStream.map(new CalculationTransformation());


		SingleOutputStreamOperator<String> pojoModelToString = calculateTotalVehicleCount.map(new PojoToTextTransformation());

		/* You can change file path according to your file organization. But On my mac,
			I faced an issue that it didn't save it to location except for the project path.
			If you are using a Mac, please consider this case.  */
		StreamingFileSink<String> fileSinkText = StreamingFileSink
				.forRowFormat(new Path("/Users/tugra/Dev/gerimedica/deneme/ibb_wifi_user_count_text_outputs"), new SimpleStringEncoder<String>())
				.build();

		pojoModelToString.addSink(fileSinkText);

		env.execute("Flink Java API Skeleton");
	}
}
