package com.flink.application.util;

import org.apache.flink.core.fs.Path;

import java.net.URISyntaxException;
import java.net.URL;

/***
 * @author M.Tugra Er
 *     10.10.2022
 */
public class CommonUtil {

    public Path getFileFromResource(String filePath) throws URISyntaxException {
        URL fileUri = getClass().getClassLoader().getResource(filePath);
        return new Path(fileUri.toURI());
    }
}
