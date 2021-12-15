/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.config.pojo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.pojo.type.SortType;

/**
 * 
 * SortTaskConfig
 */
public class SortTaskConfig {

    public static final String KEY_TASK_NAME = "taskName";
    public static final String KEY_SORT_CHANNEL_TYPE = "sortChannel.type";
    public static final String KEY_SORT_SINK_TYPE = "sortSink.type";
    public static final String KEY_SORT_SOURCE_TYPE = "sortSource.type";

    private String name;
    private SortType type;
    private List<Map<String, String>> idParams = new ArrayList<>();
    private Map<String, String> sinkParams = new HashMap<>();

    /**
     * get name
     * 
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * set name
     * 
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * get type
     * 
     * @return the type
     */
    public SortType getType() {
        return type;
    }

    /**
     * set type
     * 
     * @param type the type to set
     */
    public void setType(SortType type) {
        this.type = type;
    }

    /**
     * get idParams
     * 
     * @return the idParams
     */
    public List<Map<String, String>> getIdParams() {
        return idParams;
    }

    /**
     * set idParams
     * 
     * @param idParams the idParams to set
     */
    public void setIdParams(List<Map<String, String>> idParams) {
        this.idParams = idParams;
    }

    /**
     * get sinkParams
     * 
     * @return the sinkParams
     */
    public Map<String, String> getSinkParams() {
        return sinkParams;
    }

    /**
     * set sinkParams
     * 
     * @param sinkParams the sinkParams to set
     */
    public void setSinkParams(Map<String, String> sinkParams) {
        this.sinkParams = sinkParams;
    }

    /**
     * getFlumeConfProperties
     * 
     * @return Map
     */
    public Map<String, String> generateFlumeConfiguration() {
        Map<String, String> flumeConf = new HashMap<>();
        // channels
        this.appendChannels(flumeConf);
        // sinks
        this.appendSinks(flumeConf);
        // sources
        this.appendSources(flumeConf);
        return flumeConf;
    }

    /**
     * appendChannels
     * 
     * @param flumeConf
     */
    private void appendChannels(Map<String, String> flumeConf) {
        StringBuilder builder = new StringBuilder();
        String channelName = name + "Channel";
        flumeConf.put(name + ".channels", channelName);
        String prefix = builder.append(name).append(".channels.").append(channelName).append(".").toString();
        builder.setLength(0);
        String channelType = builder.append(prefix).append("type").toString();
        String channelClass = CommonPropertiesHolder.getString(KEY_SORT_CHANNEL_TYPE);
        flumeConf.put(channelType, channelClass);
        this.appendCommon(flumeConf, prefix, null);
    }

    /**
     * appendCommon
     * 
     * @param flumeConf
     * @param prefix
     * @param componentParams
     */
    private void appendCommon(Map<String, String> flumeConf, String prefix, Map<String, String> componentParams) {
        StringBuilder builder = new StringBuilder();
        String taskName = builder.append(prefix).append(KEY_TASK_NAME).toString();
        flumeConf.put(taskName, name);
        // CommonProperties
        for (Entry<String, String> entry : CommonPropertiesHolder.get().entrySet()) {
            builder.setLength(0);
            String key = builder.append(prefix).append(entry.getKey()).toString();
            flumeConf.put(key, entry.getValue());
        }
        // componentParams
        if (componentParams != null) {
            for (Entry<String, String> entry : componentParams.entrySet()) {
                builder.setLength(0);
                String key = builder.append(prefix).append(entry.getKey()).toString();
                flumeConf.put(key, entry.getValue());
            }
        }
    }

    /**
     * appendSinks
     * 
     * @param flumeConf
     */
    private void appendSinks(Map<String, String> flumeConf) {
        // sinks
        String sinkName = name + "Sink";
        flumeConf.put(name + ".sinks", sinkName);
        StringBuilder builder = new StringBuilder();
        String prefix = builder.append(name).append(".sinks.").append(sinkName).append(".").toString();
        // type
        builder.setLength(0);
        String sinkType = builder.append(prefix).append("type").toString();
        String sinkClass = CommonPropertiesHolder.getString(KEY_SORT_SINK_TYPE);
        flumeConf.put(sinkType, sinkClass);
        // channel
        builder.setLength(0);
        String channelKey = builder.append(prefix).append("channel").toString();
        String channelName = name + "Channel";
        flumeConf.put(channelKey, channelName);
        //
        this.appendCommon(flumeConf, prefix, sinkParams);
    }

    /**
     * appendSources
     * 
     * @param flumeConf
     */
    private void appendSources(Map<String, String> flumeConf) {
        // sources
        String sourceName = name + "Source";
        flumeConf.put(name + ".sources", sourceName);
        StringBuilder builder = new StringBuilder();
        String prefix = builder.append(name).append(".sources.").append(sourceName).append(".").toString();
        // type
        builder.setLength(0);
        String sourceType = builder.append(prefix).append("type").toString();
        String sourceClass = CommonPropertiesHolder.getString(KEY_SORT_SOURCE_TYPE);
        flumeConf.put(sourceType, sourceClass);
        // channel
        builder.setLength(0);
        String channelKey = builder.append(prefix).append("channels").toString();
        String channelName = name + "Channel";
        flumeConf.put(channelKey, channelName);
        // selector.type
        builder.setLength(0);
        String selectorTypeKey = builder.append(prefix).append("selector.type").toString();
        flumeConf.put(selectorTypeKey, "org.apache.flume.channel.ReplicatingChannelSelector");
        //
        this.appendCommon(flumeConf, prefix, null);
    }
}
