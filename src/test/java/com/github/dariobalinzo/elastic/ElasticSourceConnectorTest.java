/*
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

package com.github.dariobalinzo.elastic;


import com.github.dariobalinzo.ElasticSourceConnector;
import com.github.dariobalinzo.TestContainersContext;
import com.github.dariobalinzo.task.ElasticSourceTaskConfig;

import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ElasticSourceConnectorTest extends TestContainersContext {

    @Test
    public void shouldGetAListOfTasks() throws IOException {
        //given
        ElasticSourceConnector connector = new ElasticSourceConnector();
        connector.start(getConf());
        insertMockData(1, TEST_INDEX + 1);
        insertMockData(2, TEST_INDEX + 2);
        insertMockData(3, TEST_INDEX + 3);
        insertMockData(4, TEST_INDEX + 4);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }

        //when
        int maxTasks = 3;
        List<Map<String, String>> taskList = connector.taskConfigs(maxTasks);

        //then
        assertEquals(maxTasks, taskList.size());
        assertNotNull(connector.version());
        connector.stop();
    }

    @Test
    public void shouldGetTaskFromFixedList() {
        //given
        ElasticSourceConnector connector = new ElasticSourceConnector();
        Map<String, String> conf = getConf();
        conf.remove(ElasticSourceTaskConfig.INDEX_PREFIX_CONFIG);
        conf.put(ElasticSourceTaskConfig.INDEX_NAMES_CONFIG, "index1,index2,index3");
        connector.start(conf);

        //when
        int maxTasks = 3;
        List<Map<String, String>> taskList = connector.taskConfigs(maxTasks);

        //then
        assertEquals(maxTasks, taskList.size());
        connector.stop();
    }
}
