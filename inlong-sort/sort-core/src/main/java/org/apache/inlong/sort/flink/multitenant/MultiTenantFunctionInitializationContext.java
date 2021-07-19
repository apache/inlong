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

package org.apache.inlong.sort.flink.multitenant;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;

/**
 * It's a simple proxy to allow multiple Functions using operator state in one operator.
 */
public class MultiTenantFunctionInitializationContext implements FunctionInitializationContext {

    private final FunctionInitializationContext parentContext;

    private final MultiTenantOperatorStateStore tenantOperatorStateStore;

    public MultiTenantFunctionInitializationContext(long tenantId, FunctionInitializationContext parentContext) {
        this.parentContext = checkNotNull(parentContext);
        tenantOperatorStateStore = new MultiTenantOperatorStateStore(tenantId, parentContext.getOperatorStateStore());
    }

    @Override
    public boolean isRestored() {
        // TODO, optimize it to check restoration for each tenant
        return parentContext.isRestored();
    }

    @Override
    public OperatorStateStore getOperatorStateStore() {
        return tenantOperatorStateStore;
    }

    @Override
    public KeyedStateStore getKeyedStateStore() {
        throw new UnsupportedOperationException("Not supported yet");
    }

    private static String getTenantStateName(long tenantId, String stateName) {
        return String.format("%d_%s", tenantId, stateName);
    }

    // TODO, optimize the performance
    private static class MultiTenantOperatorStateStore implements OperatorStateStore {

        private final long tenantId;

        private final OperatorStateStore operatorStateStore;

        public MultiTenantOperatorStateStore(long tenantId, OperatorStateStore operatorStateStore) {
            this.tenantId = tenantId;
            this.operatorStateStore = checkNotNull(operatorStateStore);
        }

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> mapStateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException("Not supported yet");
        }

        @Override
        public <S> ListState<S> getListState(ListStateDescriptor<S> listStateDescriptor) throws Exception {
            return operatorStateStore
                    .getListState(new MultiTenantListStateDescriptor<>(tenantId, listStateDescriptor));
        }

        @Override
        public <S> ListState<S> getUnionListState(ListStateDescriptor<S> listStateDescriptor) throws Exception {
            return operatorStateStore
                    .getUnionListState(new MultiTenantListStateDescriptor<>(tenantId, listStateDescriptor));
        }

        @Override
        public Set<String> getRegisteredStateNames() {
            final String prefix = tenantId + "_";
            return operatorStateStore.getRegisteredStateNames().stream()
                    .filter(stateName -> stateName.startsWith(prefix))
                    .map(stateName -> StringUtils.removeStart(stateName, prefix)).collect(Collectors.toSet());
        }

        @Override
        public Set<String> getRegisteredBroadcastStateNames() {
            return Collections.emptySet();
        }

        @Override
        public <S> ListState<S> getOperatorState(ListStateDescriptor<S> listStateDescriptor) throws Exception {
            return operatorStateStore
                    .getListState(new MultiTenantListStateDescriptor<S>(tenantId, listStateDescriptor));
        }

        @Override
        public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
            return operatorStateStore.getSerializableListState(getTenantStateName(tenantId, stateName));
        }
    }

    private static class MultiTenantListStateDescriptor<T> extends ListStateDescriptor<T> {

        private static final long serialVersionUID = -8516254847853796880L;

        public MultiTenantListStateDescriptor(long tenantId, ListStateDescriptor<T> listStateDescriptor) {
            super(getTenantStateName(tenantId, listStateDescriptor.getName()),
                    listStateDescriptor.getElementSerializer());
        }
    }
}
