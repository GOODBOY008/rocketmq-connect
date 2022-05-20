/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.connect.oceanbase.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.ArrayList;
import java.util.List;
import org.apache.connect.oceanbase.config.OceanBaseConnectConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OceanBaseConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private KeyValue config;

    @Override
    public void validate(KeyValue config) {
        for (String requestKey : OceanBaseConnectConfig.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                throw new ConnectException("Request config key: " + requestKey);
            }
        }
    }

    @Override
    public void init(KeyValue config) {
        this.config = config;
    }

    @Override
    public void stop() {

    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override public List<KeyValue> taskConfigs(int maxTasks) {
        // TODO: 2022/5/20 split table to multiple tasks.
        List<KeyValue> config = new ArrayList<>();
        config.add(this.config);
        return config;
    }

    @Override public Class<? extends Task> taskClass() {
        return OceanBaseSourceTask.class;
    }
}
