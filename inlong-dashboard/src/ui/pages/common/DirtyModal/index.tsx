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

import React, { useEffect, useState } from 'react';
import { Button, message, Modal, Tabs, TabsProps } from 'antd';
import { ModalProps } from 'antd/es/modal';
import i18n from '@/i18n';
import HighTable from '@/ui/components/HighTable';

import dayjs from 'dayjs';
import request from '@/core/utils/request';
import { useForm } from 'antd/es/form/Form';
import FormGenerator from '@/ui/components/FormGenerator';
import Charts from '@/ui/components/Charts';
import { genStatusTag } from '@/ui/pages/common/DirtyModal/conf';

export interface Props extends ModalProps {
  id?: number;
}
const Comp: React.FC<Props> = ({ ...modalProps }) => {
  const [form1] = useForm();
  const [form2] = useForm();
  const [loading, setLoading] = useState(false);
  const getColumns = [
    {
      title: i18n.t('meta.Sinks.DirtyData.DataFlowId'),
      dataIndex: 'dataFlowId',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.GroupId'),
      dataIndex: 'groupId',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.StreamId'),
      dataIndex: 'streamId',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.ReportTime'),
      dataIndex: 'reportTime',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.DataTime'),
      dataIndex: 'dataTime',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.ServerType'),
      dataIndex: 'serverType',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.DirtyType'),
      dataIndex: 'dirtyType',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.DirtyMessage'),
      dataIndex: 'dirtyMessage',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.ExtInfo'),
      dataIndex: 'extInfo',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.DirtyData'),
      dataIndex: 'dirtyData',
      width: 90,
    },
    {
      title: i18n.t('meta.Sinks.DirtyData.DirtyDataPartition'),
      dataIndex: 'dirtyDataPartition',
      width: 90,
    },
  ];

  const defaultDetailOptions = {
    keyword: '',
    dataCount: '10',
    dirtyType: '',
    serverType: '',
    startTime: dayjs().format('YYYYMMDD'),
    endTime: dayjs().format('YYYYMMDD'),
  };
  const defaultTrendOptions = {
    dataTimeUnit: '',
    dirtyType: '',
    serverType: 'D',
    startTime: dayjs().format('YYYYMMDD'),
    endTime: dayjs().format('YYYYMMDD'),
  };
  const [options, setOptions] = useState(defaultDetailOptions);
  const [trendOptions, setTrendOption] = useState(defaultTrendOptions);
  const [data, setData] = useState([]);
  const [trendData, setTrendData] = useState([]);
  const [tabValue, setTabValue] = useState('detail');
  useEffect(() => {
    if (modalProps.open) {
      if (tabValue === 'detail') {
        setOptions(defaultDetailOptions);
        form1.resetFields();
        getTaskResult().then(item => {
          setData(item);
        });
      }
      if (tabValue === 'trend') {
        setOptions(defaultDetailOptions);
        form2.resetFields();
        form2.setFieldsValue({
          dataTimeUnit: 'D',
        });
        getTrendData().then(item => {
          setTrendData(item);
        });
      }
    }
  }, [modalProps.open, tabValue]);
  const [messageApi, contextHolder] = message.useMessage();
  const warning = () => {
    messageApi.open({
      type: 'warning',
      content:
        tabValue === 'detail'
          ? i18n.t('meta.Sinks.DirtyData.DirtyDetailWarning')
          : i18n.t('meta.Sinks.DirtyData.DirtyTrendWarning'),
    });
  };
  const getTaskResult = async () => {
    setLoading(true);
    const taskId = await getTaskId();
    const status = await request({
      url: '/sink/SqlTaskStatus/' + taskId,
      method: 'GET',
    });
    if (status === 'success') {
      const data = await request({
        url: '/sink/getDirtyData/' + taskId,
        method: 'GET',
      });
      setLoading(false);
      return data;
    } else {
      setLoading(false);
      warning();
    }
    return [];
  };
  const getTaskId = async () => {
    const data = await request({
      url: '/sink/listDirtyData',
      method: 'POST',
      data: {
        ...options,
        startTime: options.startTime ? dayjs(options.startTime).format('YYYYMMDD') : '',
        endTime: options.endTime ? dayjs(options.endTime).format('YYYYMMDD') : '',
        sinkIdList: [modalProps.id],
      },
    });
    return data.taskId;
  };

  const getTrendData = async () => {
    const taskId = await getTrendTaskId();
    const status = await request({
      url: '/sink/SqlTaskStatus/' + taskId,
      method: 'GET',
    });

    if (status === 'success') {
      const data = await request({
        url: '/sink/getDirtyDataTrend/' + taskId,
        method: 'GET',
      });
      return data;
    } else {
      warning();
    }
    return [];
  };

  const getTrendTaskId = async () => {
    const data = await request({
      url: '/sink/listDirtyDataTrend',
      method: 'POST',
      data: {
        ...trendOptions,
        sinkIdList: [modalProps.id],
      },
    });
    return data.taskId;
  };
  const onSearch = async () => {
    await form1.validateFields();
    await getTaskResult().then(item => {
      setData(item);
    });
  };
  const onTrendSearch = async () => {
    await form2.validateFields();
    await getTrendData().then(item => {
      setTrendData(item);
    });
  };

  const getDetailFilterFormContent = defaultValues => [
    {
      type: 'input',
      label: i18n.t('meta.Sinks.DirtyData.Search.KeyWord'),
      props: {
        placeholder: i18n.t('meta.Sinks.DirtyData.Search.KeyWordHelp'),
      },
    },
    {
      label: i18n.t('meta.Sinks.DirtyData.DataCount'),
      type: 'input',
      name: 'dataCount',
    },
    {
      label: i18n.t('meta.Sinks.DirtyData.Search.DirtyType'),
      type: 'select',
      name: 'dirtyType',
      props: {
        options: [
          {
            label: i18n.t('meta.Sinks.DirtyData.DirtyType.DeserializeError'),
            value: 'DeserializeError',
          },
          {
            label: i18n.t('meta.Sinks.DirtyData.DirtyType.FieldMappingError'),
            value: 'FieldMappingError',
          },
          {
            label: i18n.t('meta.Sinks.DirtyData.DirtyType.LoadError'),
            value: 'LoadError',
          },
        ],
      },
    },
    {
      label: i18n.t('meta.Sinks.DirtyData.Search.ServerType'),
      type: 'select',
      name: 'serverType',
      props: {
        options: [
          {
            label: 'Undefined',
            value: 'Undefined',
          },
          {
            label: 'TubeMQ',
            value: 'TubeMQ',
          },
          {
            label: 'Iceberg',
            value: 'Iceberg',
          },
        ],
      },
    },
    {
      type: 'datepicker',
      label: i18n.t('pages.GroupDetail.Audit.StartDate'),
      name: 'startTime',
      initialValue: dayjs(options.startTime),
      props: {
        allowClear: true,
        format: 'YYYYMMDD',
      },
      rules: [
        { required: true },
        ({ getFieldValue }) => ({
          validator(_, value) {
            if (Boolean(value)) {
              if (value.isAfter(dayjs())) {
                return Promise.reject(new Error(i18n.t('meta.Sinks.DirtyData.StartTimeError')));
              }
            }
            return Promise.resolve();
          },
        }),
      ],
    },
    {
      type: 'datepicker',
      label: i18n.t('pages.GroupDetail.Audit.EndDate'),
      name: 'endTime',
      initialValue: dayjs(options.endTime),
      props: values => {
        return {
          allowClear: true,
          format: 'YYYYMMDD',
        };
      },
      rules: [
        { required: true },
        ({ getFieldValue }) => ({
          validator(_, value) {
            if (Boolean(value)) {
              if (value.isAfter(dayjs())) {
                return Promise.reject(new Error(i18n.t('endTimeNotGreaterThanStartTime')));
              }
              const timeDiff = value.diff(getFieldValue('startDate'), 'day');
              if (timeDiff <= 7) {
                return Promise.resolve();
              }
              return Promise.reject(new Error(i18n.t('meta.Sinks.DirtyData.TimeIntervalError')));
            }
            return Promise.resolve();
          },
        }),
      ],
    },
    {
      type: (
        <Button type="primary" onClick={onSearch}>
          {i18n.t('basic.Search')}
        </Button>
      ),
    },
  ];
  const getTendFilterFormContent = defaultValues => [
    {
      label: i18n.t('meta.Sinks.DirtyData.Search.DirtyType'),
      type: 'select',
      name: 'dirtyType',
      props: {
        options: [
          {
            label: 'DeserializeError',
            value: 'DeserializeError',
          },
          {
            label: 'FieldMappingError',
            value: 'FieldMappingError',
          },
          {
            label: 'LoadError',
            value: 'LoadError',
          },
        ],
      },
    },
    {
      label: i18n.t('meta.Sinks.DirtyData.Search.ServerType'),
      type: 'select',
      name: 'serverType',
      props: {
        options: [
          {
            label: 'TubeMQ',
            value: 'TubeMQ',
          },
          {
            label: 'Iceberg',
            value: 'Iceberg',
          },
        ],
      },
    },
    {
      label: i18n.t('meta.Sinks.DirtyTrend.DataTimeUnit'),
      type: 'select',
      name: 'dataTimeUnit',
      initialValue: 'D',
      props: {
        options: [
          {
            label: i18n.t('meta.Sinks.DirtyTrend.Day'),
            value: 'D',
          },
          {
            label: i18n.t('meta.Sinks.DirtyTrend.Hour'),
            value: 'H',
          },
        ],
      },
    },

    {
      type: 'datepicker',
      label: i18n.t('pages.GroupDetail.Audit.StartDate'),
      name: 'startTime',
      props: values => {
        return {
          allowClear: true,
          showTime: values.dataTimeUnit === 'H',
          format: values.dataTimeUnit === 'D' ? 'YYYYMMDD' : 'YYYYMMDDHH',
        };
      },
      initialValue: dayjs(trendOptions.startTime),
      rules: [
        { required: true },
        ({ getFieldValue }) => ({
          validator(_, value) {
            if (Boolean(value)) {
              if (value.isAfter(dayjs())) {
                return Promise.reject(new Error(i18n.t('meta.Sinks.DirtyData.StartTimeError')));
              }
            }
            return Promise.resolve();
          },
        }),
      ],
    },
    {
      type: 'datepicker',
      label: i18n.t('pages.GroupDetail.Audit.EndDate'),
      name: 'endTime',
      initialValue: dayjs(trendOptions.endTime),
      props: values => {
        return {
          allowClear: true,
          showTime: values.dataTimeUnit === 'H',
          format: values.dataTimeUnit === 'D' ? 'YYYYMMDD' : 'YYYYMMDDHH',
        };
      },
      rules: [
        { required: true },
        ({ getFieldValue }) => ({
          validator(_, value) {
            if (Boolean(value)) {
              if (value.isAfter(dayjs())) {
                return Promise.reject(
                  new Error(i18n.t('meta.Sinks.DirtyData.endTimeNotGreaterThanStartTime')),
                );
              }
              const timeDiff = value.diff(getFieldValue('startTime'), 'day');
              if (timeDiff <= 7) {
                return Promise.resolve();
              }
              return Promise.reject(new Error(i18n.t('meta.Sinks.DirtyData.TimeIntervalError')));
            }
            return Promise.resolve();
          },
        }),
      ],
    },
    {
      type: (
        <Button type="primary" onClick={onTrendSearch}>
          {i18n.t('basic.Search')}
        </Button>
      ),
    },
  ];

  const onFilter = allValues => {
    setOptions(prev => ({
      ...prev,
      ...allValues,
      startTime: allValues.startTime ? +allValues.startTime.$d : '',
      endTime: allValues.endTime ? +allValues.endTime.$d : '',
    }));
  };
  const onTrendFilter = allValues => {
    setTrendOption(prev => ({
      ...prev,
      ...allValues,
      startTime: allValues.startTime
        ? allValues.dataTimeUnit === 'H'
          ? dayjs(allValues.startTime.$d).format('YYYYMMDDHH')
          : dayjs(allValues.startTime.$d).format('YYYYMMDD')
        : '',
      endTime: allValues.endTime
        ? allValues.dataTimeUnit === 'H'
          ? dayjs(allValues.endTime.$d).format('YYYYMMDDHH')
          : dayjs(allValues.endTime.$d).format('YYYYMMDD')
        : '',
    }));
  };
  const scroll = { x: 2000 };
  const toChartData = trendData => {
    return {
      legend: {
        data: trendData.map(item => item.reportTime),
      },
      tooltip: {
        trigger: 'axis',
      },
      xAxis: {
        type: 'category',
        data: trendData.map(item => item.reportTime),
      },
      yAxis: {
        type: 'value',
      },
      series: trendData.map(item => ({
        name: item.reportTime,
        type: 'line',
        data: trendData.map(item => item.count),
      })),
    };
  };
  const items: TabsProps['items'] = [
    {
      key: 'detail',
      label: i18n.t('meta.Sinks.DirtyData.Detail'),
      children: (
        <HighTable
          filterForm={{
            form: form1,
            content: getDetailFilterFormContent(options),
            style: { gap: 10 },
            onFilter: onFilter,
          }}
          table={{
            columns: getColumns,
            rowKey: 'id',
            size: 'small',
            dataSource: data,
            scroll: scroll,
            loading,
          }}
        />
      ),
    },
    {
      key: 'trend',
      label: i18n.t('meta.Sinks.DirtyData.Trend'),
      children: (
        <>
          <FormGenerator
            form={form2}
            layout="inline"
            content={getTendFilterFormContent(trendOptions)}
            style={{ gap: 10 }}
            onFilter={onTrendFilter}
          />
          <Charts height={400} option={toChartData(trendData)} forceUpdate={true} />
        </>
      ),
    },
  ];
  const onTabChange = (key: string) => {
    setTabValue(key);
  };
  useEffect(() => {
    onTabChange('detail');
  }, [modalProps.open]);
  return (
    <>
      {contextHolder}
      <Modal
        {...modalProps}
        title={i18n.t('meta.Sinks.DirtyData')}
        width={1200}
        footer={null}
        afterClose={() => {
          onTabChange('detail');
        }}
      >
        <div style={{ marginBottom: 40 }}>
          <Tabs
            defaultActiveKey="detail"
            activeKey={tabValue}
            items={items}
            onChange={onTabChange}
          />
        </div>
      </Modal>
    </>
  );
};

export default Comp;
