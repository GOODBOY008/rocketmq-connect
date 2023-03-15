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
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.connect.oceanbase.config.ConfigUtil;
import org.apache.connect.oceanbase.config.OceanBaseConnectConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OceanBaseSourceTask extends SourceTask {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private OceanBaseConnectConfig oceanBaseConnectConfig;
    private final List<LogMessage> logMessageBuffer = new LinkedList<>();
    private transient LogProxyClient logProxyClient;
    private final BlockingQueue<ConnectRecord> connectRecordQueue;
    private final AtomicBoolean snapshotCompleted = new AtomicBoolean(false);
    private transient List<Long> offsetState;
    private transient volatile long resolvedTimestamp;
    private final String logProxyHost;
    private final int logProxyPort;
    private final ClientConf logProxyClientConf;
    private final ObReaderConfig obReaderConfig;
    private final boolean snapshot;
    private final Duration connectTimeout;
    private transient Set<String> tableSet;
    private final String hostname;
    private final Integer port;
    private final String username;
    private final String password;


    public OceanBaseSourceTask(BlockingQueue<ConnectRecord> connectRecordQueue, List<Long> offsetState, long resolvedTimestamp, OceanBaseConnectConfig oceanBaseConnectConfig, String logProxyHost, int logProxyPort, ClientConf logProxyClientConf, ObReaderConfig obReaderConfig, boolean snapshot, Duration connectTimeout, Set<String> tableSet, String hostname, Integer port, String username, String password) {
        this.connectRecordQueue = connectRecordQueue;
        this.offsetState = offsetState;
        this.oceanBaseConnectConfig = oceanBaseConnectConfig;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.logProxyClientConf = logProxyClientConf;
        this.obReaderConfig = obReaderConfig;
        this.snapshot = snapshot;
        this.connectTimeout = connectTimeout;
        this.tableSet = new HashSet<>();
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.resolvedTimestamp = -1;
    }


    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void start(SourceTaskContext sourceTaskContext) {
        super.start(sourceTaskContext);
        try {
            readChangeEvents();
            if (shouldReadSnapshot()) {
                snapshotCompleted.set(false);
                readSnapshotRecords();
                snapshotCompleted.set(true);
            }
        } catch (Exception e) {
            logger.error("Start log proxy client failed", e);
            stop();
        }
    }

    protected void readSnapshotRecords() throws SQLException {
        try (OceanBaseConnection snapshotConnection = new OceanBaseConnection(
                hostname,
                port,
                username,
                password,
                connectTimeout,
                getClass().getClassLoader())) {
            for (String table : tableSet) {
                String[] schema = table.split("\\.");
                String fullName = String.format("`%s`.`%s`", schema[0], schema[1]);
                String selectSql = "SELECT * FROM " + fullName;
                logger.info("Start to read snapshot from {}", fullName);
                snapshotConnection
                        .query(
                                selectSql,
                                rs -> {
                                    ResultSetMetaData metaData = rs.getMetaData();
                                    while (rs.next()) {
                                        Map<String, Object> fieldMap = new HashMap<>();
                                        for (int i = 0; i < metaData.getColumnCount(); i++) {
                                            fieldMap.put(
                                                    metaData.getColumnName(i + 1), rs.getObject(i + 1));
                                        }
                                        try {
                                            // TODO: 2023/3/15 0015  add snapshot record
                                        } catch (Exception e) {
                                            logger.error("Deserialize snapshot record failed ", e);
                                            throw new ConnectException(e);
                                        }
                                    }
                                });
                logger.info("Read snapshot from {} finished", fullName);
            }
        }
        snapshotCompleted.set(true);
    }


    @Override
    public void init(KeyValue config) {
        oceanBaseConnectConfig = new OceanBaseConnectConfig();
        ConfigUtil.load(config, this.oceanBaseConnectConfig);
    }

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        List<ConnectRecord> res = new ArrayList<>();
        if (connectRecordQueue.drainTo(res, 20) == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
        }
        return res;
    }

    @Override
    public void stop() {
        logger.info("shut down.....");
        logProxyClient.stop();
    }

    protected void readChangeEvents() throws InterruptedException, TimeoutException {
        if (resolvedTimestamp > 0) {
            obReaderConfig.updateCheckpoint(Long.toString(resolvedTimestamp));
            logger.info("Read change events from resolvedTimestamp: {}", resolvedTimestamp);
        }

        logProxyClient =
                new LogProxyClient(logProxyHost, logProxyPort, obReaderConfig, logProxyClientConf);

        final CountDownLatch latch = new CountDownLatch(1);

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
                                if (!shouldReadSnapshot() || snapshotCompleted.get()) {
                                    logMessageBuffer.forEach(
                                            msg -> {
                                                try {
                                                    // TODO: 2023/3/15 return connectRecord
                                                } catch (Exception e) {
                                                    throw new ConnectException(e);
                                                }
                                            });
                                    logMessageBuffer.clear();
                                    long timestamp = getCheckpointTimestamp(message);
                                    if (timestamp > resolvedTimestamp) {
                                        resolvedTimestamp = timestamp;
                                    }
                                }
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
        if (!latch.await(connectTimeout.getSeconds(), TimeUnit.SECONDS)) {
            throw new TimeoutException("Timeout to receive messages in RecordListener");
        }
        logger.info("LogProxyClient packet processing started");
    }


    /**
     * Get log message checkpoint timestamp in seconds. Refer to 'globalSafeTimestamp' in {@link
     * LogMessage}.
     *
     * @param message Log message.
     * @return Timestamp in seconds.
     */
    private long getCheckpointTimestamp(LogMessage message) {
        long timestamp = -1;
        try {
            if (DataMessage.Record.Type.HEARTBEAT.equals(message.getOpt())) {
                timestamp = Long.parseLong(message.getTimestamp());
            } else {
                timestamp = message.getFileNameOffset();
            }
        } catch (Throwable t) {
            logger.error("Failed to get checkpoint from log message", t);
        }
        return timestamp;
    }

    private boolean shouldReadSnapshot() {
        return resolvedTimestamp == -1 && snapshot;
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }
}
