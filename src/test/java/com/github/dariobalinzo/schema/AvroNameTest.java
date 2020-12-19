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


import org.junit.Assert;
import org.junit.Test;

public class AvroNameTest {

    @Test
    public void shouldCreateValidAvroNames() {
        //given
        String invalidName = "foo.bar";
        FieldNameConverter converter = new AvroName();

        //when
        String validName = converter.from(invalidName);
        String validNamePrefix = converter.from("prefix", invalidName);
        String startByNumber = converter.from("1invalid");

        //then
        Assert.assertEquals("foobar", validName);
        Assert.assertEquals("prefixfoobar", validNamePrefix);
        Assert.assertEquals("avro1invalid", startByNumber);
    }

}
