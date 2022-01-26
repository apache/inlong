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

package org.apache.inlong.manager.service.core.plugin;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.inlong.manager.common.plugin.Plugin;
import org.apache.inlong.manager.common.plugin.PluginBinder;
import org.apache.inlong.manager.common.plugin.PluginDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PluginService {

    public static final String DEFAULT_PLUGIN_LOC = "/plugins";

    @Setter
    @Getter
    @Value("${plugin.location?:''}")
    private String pluginLoc;

    @Getter
    @Autowired
    private List<PluginBinder> pluginBinders;

    @Getter
    private List<Plugin> plugins = new ArrayList<>();

    public PluginService() {
        if (StringUtils.isBlank(pluginLoc)) {
            pluginLoc = DEFAULT_PLUGIN_LOC;
        }
        pluginReload();
    }

    public void pluginReload() {
        Path path = Paths.get(pluginLoc).toAbsolutePath();
        log.info("search for plugin in {}", pluginLoc);
        if (!path.toFile().exists()) {
            log.warn("plugin directory not found");
            return;
        }
        PluginClassLoader pluginLoader = PluginClassLoader.getFromPluginUrl(path.toString(),
                Thread.currentThread().getContextClassLoader());
        Map<String, PluginDefinition> pluginDefinitions = pluginLoader.getPluginDefinitions();
        if (MapUtils.isEmpty(pluginDefinitions)) {
            log.warn("pluginDefinition not found in {}", pluginLoc);
            return;
        }
        List<Plugin> plugins = new ArrayList<>();
        for (PluginDefinition pluginDefinition : pluginDefinitions.values()) {
            String pluginClassName = pluginDefinition.getPluginClass();
            try {
                Class pluginClass = pluginLoader.loadClass(pluginClassName);
                Object plugin = pluginClass.getDeclaredConstructor().newInstance();
                plugins.add((Plugin) plugin);
            } catch (Throwable e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        this.plugins.addAll(plugins);
        for (PluginBinder pluginBinder : pluginBinders) {
            for (Plugin plugin : plugins) {
                pluginBinder.acceptPlugin(plugin);
            }
        }
    }
}
