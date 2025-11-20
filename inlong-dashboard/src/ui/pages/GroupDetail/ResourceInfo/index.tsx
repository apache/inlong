/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useEffect, useMemo, forwardRef, useState } from 'react';
import { Divider, Table, Tag, Tooltip } from 'antd';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { useRequest } from '@/ui/hooks';
import { useTranslation } from 'react-i18next';
import { CommonInterface } from '../common';
import { clusters } from '@/plugins/clusters';
import HighTable from '@/ui/components/HighTable';
import i18n from '@/i18n';

type Props = CommonInterface;

const Comp = ({ inlongGroupId, isCreate }: Props, ref) => {
  const { t } = useTranslation();

  const [form] = useForm();

  const isUpdate = useMemo(() => {
    return !!inlongGroupId;
  }, [inlongGroupId]);

  const [expandedFields, setExpandedFields] = useState<Record<string, boolean>>({});
  const [sortOption, setSortOption] = useState({
    inlongGroupId: inlongGroupId,
    inlongStreamId: '',
    pageSize: 5,
    pageNum: 1,
  });

  const { data, run: getData } = useRequest(`/group/detail/${inlongGroupId}`, {
    ready: isUpdate,
    refreshDeps: [inlongGroupId],
    formatResult: data => ({
      ...data,
    }),
  });

  const { data: sortData, run: getSortData } = useRequest(
    {
      url: '/sink/listDetail',
      method: 'post',
      data: sortOption,
    },
    {
      refreshDeps: [sortOption],
      formatResult: data => ({
        ...data,
      }),
    },
  );

  useEffect(() => {
    getData();
  }, [getData]);

  const formContent = item => {
    let content = [];
    if (data[item].constructor === Object) {
      for (const key in data[item]) {
        console.log('key:', data, item, key, data[item]);
        if (data[item][key]?.constructor === Array) {
          content.push({
            label: key,
            name: item + '_' + key,
            type: () => {
              const urlList = Array.isArray(data[item][key]) ? data[item][key] : [];
              if (urlList.length === 0) return null;

              const fieldKey = `${item}_${key}`;
              const isExpanded = expandedFields[fieldKey] || false;
              const displayCount = 10;
              const displayList = isExpanded ? urlList : urlList.slice(0, displayCount);
              const remainingCount = urlList.length - displayCount;

              return (
                <div style={{ width: '80vw' }}>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                    {displayList.map((url, index) => (
                      <Tooltip key={`url-${index}`} title={url}>
                        <Tag
                          style={{
                            maxWidth: '300px',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            cursor: 'default',
                          }}
                        >
                          {url}
                        </Tag>
                      </Tooltip>
                    ))}
                    {!isExpanded && remainingCount > 0 && (
                      <Tag
                        style={{
                          cursor: 'pointer',
                          backgroundColor: '#f0f0f0',
                          borderStyle: 'dashed',
                        }}
                        onClick={() => {
                          setExpandedFields(prev => ({
                            ...prev,
                            [fieldKey]: true,
                          }));
                        }}
                      >
                        +{remainingCount}
                      </Tag>
                    )}
                    {isExpanded && urlList.length > displayCount && (
                      <Tag
                        style={{
                          cursor: 'pointer',
                          backgroundColor: '#f0f0f0',
                          borderStyle: 'dashed',
                        }}
                        onClick={() => {
                          setExpandedFields(prev => ({
                            ...prev,
                            [fieldKey]: false,
                          }));
                        }}
                      >
                        {i18n.t('pages.GroupDetail.Resource.Collapse')}
                      </Tag>
                    )}
                  </div>
                </div>
              );
            },
            initialValue: data[item][key],
            visible: !!data[item][key],
          });
        } else {
          content.push({
            label: key,
            name: item + '_' + key,
            type: 'text',
            initialValue: data[item][key],
            visible: !!data[item][key],
          });
        }
      }
      return content;
    }
    if (data[item].constructor === Array) {
      const infoData = [];
      for (const key in data[item]) {
        infoData.push(data[item][key].url);
      }
      content.push({
        label: 'url',
        name: 'kafkaUrl',
        type: 'text',
        initialValue: infoData.join(','),
      });
      return content;
    }
  };

  const dividerInfo = data => {
    let info = [];
    for (const item in data) {
      if (
        data[item] !== null &&
        item !== 'PULSAR' &&
        item !== 'TUBEMQ' &&
        item !== 'inlongClusterTag'
      ) {
        info.push(item);
      }
    }
    return info;
  };
  const onChange = ({ current: pageNum, pageSize }) => {
    setSortOption(pre => ({ ...pre, pageNum: pageNum, pageSize: pageSize }));
  };

  const pagination = {
    pageSize: 5,
    current: sortOption.pageNum,
    total: sortData?.total,
  };

  const onFilter = allValues => {
    setSortOption(pre => ({
      ...pre,
      inlongStreamId: allValues.streamId,
    }));
  };

  const content = () => [
    {
      type: 'inputsearch',
      label: 'Stream Id',
      name: 'streamId',
      props: {
        allowClear: true,
      },
    },
  ];

  return (
    <div style={{ position: 'relative' }}>
      {data?.hasOwnProperty('inlongClusterTag') && (
        <>
          <Divider orientation="left">Cluster tag {t('pages.GroupDetail.Resource.Info')}</Divider>
          <div>
            <span>Cluster tag:</span>
            <span style={{ marginLeft: 100 }}>{data?.inlongClusterTag}</span>
          </div>
        </>
      )}
      {dividerInfo(data).map(item => {
        return (
          <>
            <Divider orientation="left" style={{ marginTop: 40 }}>
              {clusters.find(c => c.value === item)?.label || item}{' '}
              {t('pages.GroupDetail.Resource.Info')}
            </Divider>
            <FormGenerator
              form={form}
              content={formContent(item)}
              initialValues={data}
              useMaxWidth={1400}
              col={12}
            />
          </>
        );
      })}
      {data?.hasOwnProperty('PULSAR') && (
        <>
          <Divider orientation="left" style={{ marginTop: 40 }}>
            Pulsar {t('pages.GroupDetail.Resource.Info')}
          </Divider>
          <Table
            size="small"
            columns={[
              { title: 'Default Tenant', dataIndex: 'defaultTenant' },
              { title: 'Server Url', dataIndex: 'serverUrl' },
              { title: 'Admin Url', dataIndex: 'adminUrl' },
            ]}
            style={{ marginTop: 20 }}
            dataSource={data?.PULSAR}
            pagination={false}
            rowKey="name"
          ></Table>
        </>
      )}
      {data?.hasOwnProperty('TUBEMQ') && (
        <>
          <Divider orientation="left" style={{ marginTop: 40 }}>
            TubeMQ {t('pages.GroupDetail.Resource.Info')}
          </Divider>
          <Table
            size="small"
            columns={[
              { title: 'RPC Url', dataIndex: 'RPC Url' },
              { title: 'Web Url', dataIndex: 'Web url' },
            ]}
            style={{ marginTop: 20, width: 1100 }}
            dataSource={data?.TUBEMQ}
            pagination={false}
            rowKey="name"
          ></Table>
        </>
      )}
      <>
        <Divider orientation="left" style={{ marginTop: 60 }}>
          Sort {t('pages.GroupDetail.Resource.Info')}
        </Divider>
        <HighTable
          filterForm={{
            content: content(),
            onFilter,
          }}
          table={{
            columns: [
              { title: 'inlongStreamId', dataIndex: 'inlongStreamId' },
              { title: 'dataflowId', dataIndex: 'id' },
              { title: 'sinkName', dataIndex: 'sinkName' },
              { title: 'topoName', dataIndex: 'inlongClusterName' },
            ],
            style: { marginTop: 20 },
            dataSource: sortData?.list,
            pagination,
            rowKey: 'name',
            onChange,
          }}
        />
      </>
    </div>
  );
};

export default forwardRef(Comp);
