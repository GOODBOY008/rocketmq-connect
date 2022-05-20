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

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.clogproxy.client.util.ClientIdGenerator;
import com.oceanbase.oms.logmessage.LogMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.connect.oceanbase.config.ConfigUtil;
import org.apache.connect.oceanbase.config.OceanBaseConnectConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OceanBaseSourceTask extends SourceTask {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private OceanBaseConnectConfig oceanBaseConnectConfig;
    private final List<LogMessage> logMessageBuffer = new LinkedList<>();
    private transient LogProxyClient logProxyClient;

    private transient List<Long> offsetState;

    private transient volatile long resolvedTimestamp;

    @Override public void validate(KeyValue config) {

    }

    @Override public void init(KeyValue config) {
        oceanBaseConnectConfig = new OceanBaseConnectConfig();
        ConfigUtil.load(config, this.oceanBaseConnectConfig);
    }

    @Override public List<ConnectRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {
        logger.info("shut down.....");
        logProxyClient.stop();
    }

    @Override
    public void pause() {
        logger.info("pause replica task...");
        try {
            logProxyClient.wait();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void resume() {
        logger.info("resume replica task...");
        logProxyClient.notifyAll();
    }

    protected void readChangeEvents() throws InterruptedException {

        String tableWhiteList = String.format("%s.%s.%s", oceanBaseConnectConfig.getTenantName(), oceanBaseConnectConfig.getDatabaseName(), oceanBaseConnectConfig.getTableName());
        ObReaderConfig obReaderConfig = new ObReaderConfig();
        obReaderConfig.setRsList(oceanBaseConnectConfig.getRsList());
        obReaderConfig.setUsername(oceanBaseConnectConfig.getUsername());
        obReaderConfig.setPassword(oceanBaseConnectConfig.getPassword());
        obReaderConfig.setTableWhiteList(tableWhiteList);

        if (resolvedTimestamp > 0) {
            obReaderConfig.setStartTimestamp(resolvedTimestamp);
            logger.info("Read change events from resolvedTimestamp: {}", resolvedTimestamp);
        } else {

//            obReaderConfig.setStartTimestamp(startTimestamp);
//            logger.info("Read change events from startTimestamp: {}", startTimestamp);
        }

        final CountDownLatch latch = new CountDownLatch(1);

        // avoid client id duplication when starting multiple connectors in one etl
        ClientConf.USER_DEFINED_CLIENTID = ClientIdGenerator.generate() + tableWhiteList;
        logProxyClient = new LogProxyClient(oceanBaseConnectConfig.getLogProxyHost(), oceanBaseConnectConfig.getLogProxyPort(), obReaderConfig);

        logProxyClient.addListener(
            new RecordListener() {

                boolean started = false;

                @Override
                public void notify(LogMessage message) {
                    switch (message.getOpt()) {
                        case HEARTBEAT:
                        case BEGIN:
                            if (!started) {
                                started = true;
                                latch.countDown();
                            }
                            break;
                        case INSERT:
                        case UPDATE:
                        case DELETE:
                            if (!started) {
                                break;
                            }
                            logMessageBuffer.add(message);
                            break;
                        case COMMIT:
                            // flush buffer after snapshot completed
                           /* if (!shouldReadSnapshot() || snapshotCompleted.get()) {
                                logMessageBuffer.forEach(
                                    msg -> {
                                        try {
                                            deserializer.deserialize(
                                                getRecordFromLogMessage(msg),
                                                outputCollector);
                                        } catch (Exception e) {
                                            throw new FlinkRuntimeException(e);
                                        }
                                    });
                                logMessageBuffer.clear();
                                resolvedTimestamp = Long.parseLong(message.getTimestamp());
                            }*/
                            break;
                        case DDL:
                            // TODO record ddl and remove expired table schema
                            logger.trace(
                                "Ddl: {}",
                                message.getFieldList().get(0).getValue().toString());
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                "Unsupported type: " + message.getOpt());
                    }
                }

                @Override
                public void onException(LogProxyClientException e) {
                    logger.error("LogProxyClient exception", e);
                    logProxyClient.stop();
                }
            });

        logProxyClient.start();
        logger.info("LogProxyClient started");
        latch.await();
        logger.info("LogProxyClient packet processing started");
    }

}
