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

package org.apache.inlong.manager.service.thirdpart.sort;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.StorageHiveEntity;
import org.apache.inlong.manager.dao.mapper.StorageHiveEntityMapper;
import org.apache.inlong.manager.service.core.DataStreamService;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.service.workflow.newbusiness.CreateResourceWorkflowForm;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.apache.inlong.sort.ZkTools;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.DataFlowInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgCsvDeserializationInfo;
import org.apache.inlong.sort.protocol.sink.HiveSinkInfo;
import org.apache.inlong.sort.protocol.source.SourceInfo;
import org.apache.inlong.sort.protocol.source.TubeSourceInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PushHiveConfigToSortEventListener implements TaskEventListener {

    @Autowired
    private StorageService storageService;
    @Autowired
    private StorageHiveEntityMapper storageHiveMapper;
    @Autowired
    private DataStreamService dataStreamService;
    @Autowired
    private ClusterBean clusterBean;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        if (log.isDebugEnabled()) {
            log.debug("begin push hive config to sort, context={}", context);
        }

        CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) context.getProcessForm();
        BusinessInfo businessInfo = form.getBusinessInfo();
        String bid = businessInfo.getBusinessIdentifier();
        // if dsid not null, just push the config belongs to the bid and the dsid
        String dsid = form.getDataStreamIdentifier();

        List<StorageHiveEntity> storageHiveEntities = storageHiveMapper.selectByIdentifier(bid, dsid);
        for (StorageHiveEntity hiveEntity : storageHiveEntities) {
            Integer storageId = hiveEntity.getId();
            StorageHiveInfo hiveStorage = (StorageHiveInfo) storageService
                    .getById(BizConstant.STORAGE_TYPE_HIVE, storageId);
            if (log.isDebugEnabled()) {
                log.debug("hive storage info: {}", hiveStorage);
            }

            DataFlowInfo dataFlowInfo = getDataFlowInfo(businessInfo, hiveStorage);
            if (log.isDebugEnabled()) {
                log.debug("try to push hive config to sort: {}", JsonUtils.toJson(dataFlowInfo));
            }
            try {
                String zkUrl = clusterBean.getZkUrl();
                String zkRoot = clusterBean.getZkRoot();
                // push data flow info to zk
                String sortClusterName = clusterBean.getAppName();
                ZkTools.updateDataFlowInfo(dataFlowInfo, sortClusterName, storageId, zkUrl, zkRoot);
                // add storage id to zk
                ZkTools.addDataFlowToCluster(sortClusterName, storageId, zkUrl, zkRoot);
            } catch (Exception e) {
                log.error("add or update data stream information to zk failed, storageId={} ", storageId, e);
                throw new WorkflowListenerException("push hive config to sort failed, reason: " + e.getMessage());
            }
        }

        return ListenerResult.success();
    }

    private DataFlowInfo getDataFlowInfo(BusinessInfo businessInfo, StorageHiveInfo hiveStorage) {
        Stream<FieldInfo> hiveFields = hiveStorage.getHiveFieldList().stream().map(field -> {
            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(field.getFieldType().toLowerCase());
            return new FieldInfo(field.getFieldName(), formatInfo);
        });

        List<FieldInfo> sinkFields = hiveFields.collect(Collectors.toList());
        FieldInfo partitionFieldInfo = new FieldInfo(hiveStorage.getPrimaryPartition(),
                new TimestampFormatInfo("MILLIS"));
        sinkFields.add(partitionFieldInfo);

        String hiveServerUrl = hiveStorage.getJdbcUrl();
        if (hiveServerUrl != null && !hiveServerUrl.startsWith("jdbc:hive2://")) {
            hiveServerUrl = "jdbc:hive2://" + hiveServerUrl;
        }

        // dataPath = hdfsUrl + / + warehouseDir + / + dbName + .db/ + tableName
        StringBuilder dataPathBuilder = new StringBuilder();
        String hdfsUrl = hiveStorage.getHdfsDefaultFs();
        String warehouseDir = hiveStorage.getWarehouseDir();
        if (hdfsUrl.endsWith("/")) {
            dataPathBuilder.append(hdfsUrl, 0, hdfsUrl.length() - 1);
        } else {
            dataPathBuilder.append(hdfsUrl);
        }
        if (warehouseDir.endsWith("/")) {
            dataPathBuilder.append(warehouseDir, 0, warehouseDir.length() - 1);
        } else {
            dataPathBuilder.append(warehouseDir);
        }
        String dataPath = dataPathBuilder.append("/")
                .append(hiveStorage.getDbName())
                .append(".db/")
                .append(hiveStorage.getTableName())
                .toString();

        // Encapsulate the deserialization information in the source
        DataStreamInfo dataStream = dataStreamService.get(hiveStorage.getBusinessIdentifier(),
                hiveStorage.getDataStreamIdentifier());

        HiveSinkInfo.HiveFileFormat fileFormat = new HiveSinkInfo.TextFileFormat(',');
        if (dataStream.getFileDelimiter() != null) {
            char c = (char) Integer.parseInt(dataStream.getFileDelimiter());
            fileFormat = new HiveSinkInfo.TextFileFormat(c);
        }

        // encapsulate hive sink
        HiveSinkInfo hiveSinkInfo = new HiveSinkInfo(
                sinkFields.toArray(new FieldInfo[0]),
                hiveServerUrl,
                hiveStorage.getDbName(),
                hiveStorage.getTableName(),
                hiveStorage.getUsername(),
                hiveStorage.getPassword(),
                dataPath,
                Stream.of(new HiveSinkInfo.HiveTimePartitionInfo(hiveStorage.getPrimaryPartition(),
                        "yyyyMMddHH")).toArray(HiveSinkInfo.HivePartitionInfo[]::new),
                fileFormat
        );

        // data stream fields
        Stream<FieldInfo> streamFields = dataStream.getFieldList().stream().map(field -> {
            FormatInfo formatInfo = SortFieldFormatUtils.convertFieldFormat(field.getFieldType().toLowerCase());
            return new FieldInfo(field.getFieldName(), formatInfo);
        });

        String topic = businessInfo.getMqResourceObj();
        String consumeGroupName = "sort_" + businessInfo.getMqResourceObj() + "_consumer_group";
        TDMsgCsvDeserializationInfo deserializationInfo = null;
        if (BizConstant.DATA_TYPE_TEXT.equalsIgnoreCase(dataStream.getDataType())) {
            char c = (char) Integer.parseInt(dataStream.getFileDelimiter());
            deserializationInfo = new TDMsgCsvDeserializationInfo(hiveStorage.getDataStreamIdentifier(), c);
        }
        SourceInfo sourceInfo = new TubeSourceInfo(topic, clusterBean.getTubeMaster(), consumeGroupName,
                deserializationInfo, streamFields.toArray(FieldInfo[]::new));

        // push information
        return new DataFlowInfo(hiveStorage.getId(), sourceInfo, hiveSinkInfo);
    }

    @Override
    public boolean async() {
        return false;
    }
}
