/**
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo.utils;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import org.apache.kafka.connect.data.Struct;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Utils {

    public static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static List<List<String>> groupPartitions(List<String> currentIndices, int numGroups) {
        List<List<String>> result = new ArrayList<>(numGroups);
        for (int i=0; i<numGroups; ++i) {
            result.add(new ArrayList<>());
        }

        for (int i=0; i<currentIndices.size(); ++i) {
            result.get(i%numGroups).add(currentIndices.get(i));
        }

        return result;
    }

    public static List<String> getIndexList(Response indicesReply, String prefix) {

        List<String> result = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader( new InputStreamReader( indicesReply.getEntity().getContent()))) {
            String line = null;

            while ((line = reader.readLine()) != null) {
                String index = line.split("\\s+")[2];
                if (index.startsWith(prefix)) {
                    result.add(index);
                }
            }
        } catch (IOException e) {
            logger.error("error while getting indices",e);
        }

        return result;
    }


    //not all elastic names are valid avro name
    public static String filterAvroName(String elasticName) {
        return elasticName == null ? null:elasticName.replaceAll("[^a-zA-Z0-9]", "");
    }

    public static String filterAvroName(String prefix, String elasticName) {
        return elasticName == null ? prefix:prefix+elasticName.replaceAll("[^a-zA-Z0-9]", "");
    }

    /**
     * Adds the label (key, value) to the record
     *
     * @param sourceAsMap The ElasticSearch result as a Map
     * @param labelKey The key of the label to add
     * @param labelValue The value of the label to add
     * @return The labeled struct
     */
    public static Map<String, Object> addLabel(Map<String, Object> sourceAsMap, String labelKey, String labelValue) {
        if (!labelKey.isEmpty() && !labelValue.isEmpty()) {
            sourceAsMap.put(labelKey, labelValue);
        }
        return sourceAsMap;
    }
}
