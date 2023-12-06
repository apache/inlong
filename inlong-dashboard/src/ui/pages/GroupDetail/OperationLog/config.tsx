import i18n from '@/i18n';
import { Tooltip } from 'antd';
import React from 'react';

const targetList = [
  {
    label: 'Group',
    value: 'GROUP',
  },
  {
    label: 'Stream',
    value: 'STREAM',
  },
  {
    label: 'Source',
    value: 'SOURCE',
  },
  {
    label: 'Sink',
    value: 'SINK',
  },
];

const typeList = [
  {
    label: 'Create',
    value: 'CREATE',
  },
  {
    label: 'Update',
    value: 'UPDATE',
  },
  {
    label: 'Delete',
    value: 'DELETE',
  },
  {
    label: 'Get',
    value: 'GET',
  },
];

export const getFormContent = inlongGroupId => [
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.OperationLog.Stream'),
    name: 'inlongStreamId',
    props: {
      options: {
        requestAuto: true,
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/stream/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 20,
            inlongGroupId,
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list?.map(item => ({
              label: item.inlongStreamId,
              value: item.inlongStreamId,
            })),
        },
      },
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.OperationLog.OperationTarget'),
    name: 'operationTarget',
    props: {
      dropdownMatchSelectWidth: false,
      options: targetList,
    },
  },
  {
    type: 'select',
    label: i18n.t('pages.GroupDetail.OperationLog.OperationType'),
    name: 'operationType',
    props: {
      dropdownMatchSelectWidth: false,
      options: typeList,
    },
  },
];

export const getTableColumns = [
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.GroupId'),
    dataIndex: 'inlongGroupId',
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.StreamId'),
    dataIndex: 'inlongStreamId',
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.Operator'),
    dataIndex: 'operator',
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.OperationType'),
    dataIndex: 'operationType',
    render: text => typeList.find(c => c.value === text)?.label || text,
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.OperationTarget'),
    dataIndex: 'operationTarget',
    render: text => targetList.find(c => c.value === text)?.label || text,
  },
  {
    title: i18n.t('pages.GroupDetail.OperationLog.Table.Log'),
    dataIndex: 'body',
    ellipsis: true,
    render: body => (
      <Tooltip placement="topLeft" title={body}>
        {body}
      </Tooltip>
    ),
  },
];
