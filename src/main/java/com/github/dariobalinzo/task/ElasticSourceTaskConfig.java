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

package com.github.dariobalinzo.task;


import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

/**
 * Configuration options for a single ElasticSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class ElasticSourceTaskConfig extends ElasticSourceConnectorConfig {

    static ConfigDef config = baseConfigDef()
            .define(INDICES_CONFIG, Type.STRING, Importance.HIGH, INDICES_CONFIG);

    public ElasticSourceTaskConfig(Map<String, String> props) {
        super(config, props);
    }
}
