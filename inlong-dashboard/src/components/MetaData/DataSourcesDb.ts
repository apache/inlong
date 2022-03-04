import request from '@/utils/request';
import { getColsFromFields } from '@/utils/metaData';
import { ColumnsType } from 'antd/es/table';
import rulesPattern from '@/utils/pattern';

export const getDataSourcesDbFields = (
  type: 'form' | 'col' = 'form',
  { currentValues } = {} as any,
) => {
  const fileds = [
    {
      name: 'accessType',
      type: 'radio',
      label: '类型',
      initialValue: 'DB_SYNC_AGENT',
      rules: [{ required: true }],
      props: {
        options: [
          // {
          //   label: 'SQL',
          //   value: 'SQL',
          // },
          {
            label: 'BinLog',
            value: 'DB_SYNC_AGENT',
          },
        ],
      },
    },
    {
      name: 'serverName',
      type: 'select',
      label: 'DB服务器',
      rules: [{ required: true }],
      extraNames: ['serverId'],
      props: {
        // asyncValueLabel: '',
        options: {
          requestService: async () => {
            const groupData = await request({
              url: '/commonserver/getByUser',
              params: {
                serverType: 'DB',
              },
            });
            return groupData;
          },
          requestParams: {
            formatResult: result =>
              result?.map(item => ({
                ...item,
                label: item.serverName,
                value: item.serverName,
                serverId: item.id,
              })),
          },
        },
        onChange: (value, option) => ({
          serverId: option.serverId,
          dbName: option.dbName,
          clusterName: undefined,
        }),
        disabled: currentValues?.status === 101,
      },
      _inTable: true,
    },
    {
      name: 'dbName',
      type: 'input',
      label: 'DB名称',
      rules: [{ required: true }],
      _inTable: true,
    },
    {
      name: 'dbAgentIp',
      type: 'input',
      label: 'DB Agent IP',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: '请正确输入IP地址',
        },
      ],
    },
    {
      name: 'tableName',
      type: 'input',
      label: '表名称',
      rules: [{ required: true }],
      props: {
        placeholder: '多张表用（,）分隔，注意：多表的结构须完全一致',
      },
      _inTable: true,
    },
    {
      name: 'charset',
      type: 'select',
      label: '字符编码',
      rules: [{ required: true }],
      initialValue: 'UTF-8',
      props: {
        options: [
          {
            label: 'UTF-8',
            value: 'UTF-8',
          },
          {
            label: 'GBK',
            value: 'GBK',
          },
        ],
      },
      _inTable: true,
    },
    {
      name: 'skipDelete',
      type: 'radio',
      label: '跳过delete事件',
      rules: [{ required: true }],
      initialValue: 1,
      props: {
        options: [
          {
            label: '是',
            value: 1,
          },
          {
            label: '否',
            value: 0,
          },
        ],
      },
    },
    {
      name: '_startDumpPosition',
      type: 'radio',
      label: '回拨BinLog起始位置',
      initialValue: currentValues?._startDumpPosition || 0,
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: '是',
            value: 1,
          },
          {
            label: '否',
            value: 0,
          },
        ],
      },
    },
    {
      name: 'startDumpPosition.logIdentity.sourceIp',
      type: 'input',
      label: '数据库IP',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: '请正确输入IP地址',
        },
      ],
      visible: values => values?._startDumpPosition,
    },
    {
      name: 'startDumpPosition.logIdentity.sourcePort',
      type: 'inputnumber',
      label: '数据库端口号',
      props: {
        min: 1,
        max: 65535,
      },
      rules: [{ required: true }],
      visible: values => values?._startDumpPosition,
    },
    {
      name: 'startDumpPosition.entryPosition.journalName',
      type: 'input',
      label: 'BinLog文件名',
      rules: [{ required: true }],
      visible: values => values?._startDumpPosition,
    },
    {
      name: 'startDumpPosition.entryPosition.position',
      type: 'inputnumber',
      label: 'BinLog文件位置',
      rules: [{ required: true }],
      props: {
        min: 1,
        max: 1000000000,
        precision: 0,
      },
      visible: values => values?._startDumpPosition,
    },
  ];

  return type === 'col' ? getColsFromFields(fileds) : fileds;
};

export const toFormValues = data => {
  return {
    ...data,
    _startDumpPosition: data.startDumpPosition ? 1 : 0,
  };
};

export const toSubmitValues = data => {
  const output = { ...data };
  delete output._startDumpPosition;
  return {
    ...output,
    startDumpPosition: data._startDumpPosition ? output.startDumpPosition : null,
  };
};

export const dataSourcesDbColumns = getDataSourcesDbFields('col') as ColumnsType;
