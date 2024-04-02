/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package source;

import channel.DataQueue;
import entities.SourceConfig;
import entities.StartEndTime;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract source
 */
@Data
public class AbstractSource {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSource.class);
    protected final ConcurrentHashMap<Integer, ScheduledExecutorService> statTimers = new ConcurrentHashMap<>();
    protected DataQueue dataQueue;
    protected List<String> auditIds;
    protected int querySqlTimeout;
    protected DataSource dataSource;
    protected String querySql;
    protected SourceConfig sourceConfig;

    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public AbstractSource(DataQueue dataQueue) {
        this.dataQueue = dataQueue;
    }

    /**
     * Get stat cycle of minute
     *
     * @param beforeHour
     * @param dataCycle
     * @return
     */
    public List<StartEndTime> getStatCycleMinute(int beforeHour, int dataCycle) {
        List<StartEndTime> statCycleList = new LinkedList<>();
        for (int step = 0; step < 60; step = step + dataCycle) {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.HOUR_OF_DAY, -beforeHour);

            calendar.set(Calendar.MINUTE, step);
            calendar.set(Calendar.SECOND, 0);
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            StartEndTime statCycle = new StartEndTime();
            statCycle.setStartTime(dateFormat.format(calendar.getTime()));

            calendar.set(Calendar.MINUTE, step + dataCycle - 1);
            calendar.set(Calendar.SECOND, 0);
            statCycle.setEndTime(dateFormat.format(calendar.getTime()));
            statCycleList.add(statCycle);
        }
        return statCycleList;
    }

    /**
     * Get stat cycle of day
     *
     * @param beforeDay
     * @return
     */
    public List<StartEndTime> getStatCycleDay(int beforeDay) {
        StartEndTime statCycle = new StartEndTime();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -beforeDay);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        statCycle.setStartTime(dateFormat.format(calendar.getTime()));

        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        statCycle.setEndTime(dateFormat.format(calendar.getTime()));
        return new ArrayList<StartEndTime>() {

            {
                add(statCycle);
            }
        };
    }

    /**
     * Destory
     */
    public void destory() {
        try {
            dataSource.getConnection().close();
        } catch (SQLException exception) {
            LOG.error(exception.getMessage());
        }
    }
}
