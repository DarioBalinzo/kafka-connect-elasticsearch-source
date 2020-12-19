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

package com.github.dariobalinzo.schema;

public class AvroName implements FieldNameConverter {

    public String from(String elasticName) {
        return elasticName == null ? null : filterInvalidCharacters(elasticName);
    }

    public String from(String prefix, String elasticName) {
        return elasticName == null ? prefix : prefix + filterInvalidCharacters(elasticName);
    }

    private String filterInvalidCharacters(String elasticName) {
        boolean alphabetic = Character.isLetter(elasticName.charAt(0));
        if (!alphabetic) {
            elasticName = "avro" + elasticName;
        }
        return elasticName.replaceAll("[^a-zA-Z0-9]", "");
    }
}
