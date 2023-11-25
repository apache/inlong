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

import React, { useState } from 'react';
import { Modal, message, Form, Radio } from 'antd';
import { ModalProps } from 'antd/es/modal';
import { useForm } from '@/ui/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import { useTranslation } from 'react-i18next';
import request from '@/core/utils/request';
import EditableTable, { ColumnsItemProps } from '@/ui/components/EditableTable';
import i18n from '@/i18n';

export interface Props extends ModalProps {
  // When editing, use the ID to call the interface for obtaining details
  id?: string;
  inlongGroupId: string;
  inlongStreamId: string;
  defaultType?: string;
}

const Comp: React.FC<Props> = ({
  id,
  inlongGroupId,
  inlongStreamId,
  defaultType,
  ...modalProps
}) => {
  const [form] = useForm();
  const { t } = useTranslation();

  const [sourceNames, setSourcesNames] = useState([]);
  const [sinkNames, setSinkNames] = useState([]);

  const { data, run: getData } = useRequest(
    streamId => ({
      url: `/stream/getBrief`,
      params: {
        groupId: inlongGroupId,
        streamId,
      },
    }),
    {
      manual: true,
    },
  );

  const { data: sourceData, run: getSourceData } = useRequest(
    inlongStreamId => ({
      url: `/source/list`,
      method: 'POST',
      data: {
        inlongGroupId,
        inlongStreamId,
      },
    }),
    {
      manual: true,
      onSuccess: result => {
        const list = result.list.map(item => item.sourceName);
        setSourcesNames(list);
      },
    },
  );

  const { data: sinkData, run: getSinkData } = useRequest(
    inlongStreamId => ({
      url: `/sink/list`,
      method: 'POST',
      data: {
        inlongGroupId,
        inlongStreamId,
      },
    }),
    {
      manual: true,
      onSuccess: result => {
        const list = result.list.map(item => item.sinkName);
        setSinkNames(list);
      },
    },
  );

  const { data: transformData, run: getTransformData } = useRequest(
    id => ({
      url: `/transform/get/${id}`,
    }),
    {
      manual: true,
      onSuccess: result => {
        form.setFieldsValue(JSON.parse(result.transformDefinition));
      },
    },
  );

  const columns: ColumnsItemProps[] = [
    {
      title: i18n.t('pages.SynchronizeDetail.Transform.LogicOperators'),
      type: 'select',
      dataIndex: 'relationWithPost',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: 'AND',
            value: 'AND',
          },
          {
            label: 'OR',
            value: 'OR',
          },
        ],
      },
    },
    {
      title: i18n.t('pages.SynchronizeDetail.Transform.FilterFields'),
      type: 'select',
      dataIndex: 'fieldSource',
      rules: [{ required: true }],
      props: {
        options: data?.fieldList.map(item => ({
          ...item,
          label: item.fieldName,
          value: item.fieldName,
        })),
        onChange: (value, option) => {
          return {
            fieldSource: value,
            sourceField: {
              fieldName: option.fieldName,
              fieldType: option.fieldType,
            },
          };
        },
      },
    },
    {
      title: i18n.t('pages.SynchronizeDetail.Transform.Operators'),
      type: 'select',
      dataIndex: 'operationType',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: '<',
            value: 'lt',
          },
          {
            label: '>',
            value: 'gt',
          },
          {
            label: '<=',
            value: 'le',
          },
          {
            label: '>=',
            value: 'ge',
          },
          {
            label: '=',
            value: 'eq',
          },
          {
            label: '!=',
            value: 'ne',
          },
          {
            label: i18n.t('pages.SynchronizeDetail.Transform.Operators.IsNull'),
            value: 'is_null',
          },
          {
            label: i18n.t('pages.SynchronizeDetail.Transform.Operators.NotNull'),
            value: 'not_null',
          },
        ],
      },
    },
    {
      title: i18n.t('pages.SynchronizeDetail.Transform.Type'),
      type: 'select',
      dataIndex: 'type',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: i18n.t('pages.SynchronizeDetail.Transform.Type.Field'),
            value: 'false',
          },
          {
            label: i18n.t('pages.SynchronizeDetail.Transform.Type.CustomValue'),
            value: 'true',
          },
        ],
      },
    },
    {
      title: i18n.t('pages.SynchronizeDetail.Transform.ComparisonValue'),
      type: 'autocomplete',
      dataIndex: 'target',
      rules: [{ required: true }],
      props: (text, record, idx, isNew) => ({
        options:
          record.type === 'false'
            ? sinkData?.list[0]?.sinkFieldList.map(item => ({
                ...item,
                label: item.fieldName,
                value: item.fieldName,
              }))
            : null,
        onChange: (value, option) => {
          return {
            target: value,
            targetValue: {
              constant: record.type === 'true' ? true : false,
              targetField:
                record.type === 'true'
                  ? record.sourceField
                  : { fieldName: option.fieldName, fieldType: option.fieldType },
              targetConstant: value,
            },
          };
        },
      }),
    },
  ];

  const onOk = async () => {
    const values = await form.validateFields();
    const submitData = {
      ...values,
      inlongGroupId,
      inlongStreamId,
      transformDefinition: JSON.stringify({
        filterStrategy: values.filterStrategy,
        filterMode: 'RULE',
        filterRules: values.filterRules,
      }),
    };
    const isUpdate = Boolean(id);
    if (isUpdate) {
      submitData.id = id;
      submitData.version = transformData?.version;
    }
    await request({
      url: `/transform/${isUpdate ? 'update' : 'save'}`,
      method: 'POST',
      data: {
        ...submitData,
        postNodeNames: sinkNames.join(','),
        preNodeNames: sourceNames.join(','),
        transformName: inlongGroupId + '_transform',
        transformType: 'filter',
        fieldList: data?.fieldList,
      },
    });
    modalProps?.onOk(submitData);
    message.success(t('pages.GroupDetail.Sources.SaveSuccessfully'));
  };

  useUpdateEffect(() => {
    if (modalProps.open) {
      getData(inlongStreamId);
      getSourceData(inlongStreamId);
      getSinkData(inlongStreamId);
      if (id) {
        getTransformData(id);
      }
    } else {
      form.resetFields();
    }
  }, [modalProps.open]);

  return (
    <>
      <Modal
        {...modalProps}
        title={id ? t('pages.GroupDetail.Sources.Edit') : t('pages.GroupDetail.Sources.Create')}
        width={1200}
        onOk={onOk}
      >
        <Form form={form} labelAlign={'left'}>
          <Form.Item
            name="filterStrategy"
            initialValue={'RETAIN'}
            wrapperCol={{ span: 8, offset: 1 }}
            label={i18n.t('pages.SynchronizeDetail.Transform.FilterAction')}
            tooltip={i18n.t('pages.SynchronizeDetail.Transform.FilterAction.Tooltip')}
          >
            <Radio.Group>
              <Radio value="RETAIN" checked>
                {i18n.t('pages.SynchronizeDetail.Transform.FilterAction.ReserveData')}
              </Radio>
              <Radio value="REMOVE">
                {i18n.t('pages.SynchronizeDetail.Transform.FilterAction.RemoveData')}
              </Radio>
            </Radio.Group>
          </Form.Item>
          <Form.Item
            name={'filterRules'}
            labelCol={{ span: 2 }}
            label={i18n.t('pages.SynchronizeDetail.Transform.FilterRules')}
            wrapperCol={{ offset: 1 }}
          >
            <EditableTable
              columns={columns}
              dataSource={data}
              canBatchAdd={true}
              upsetByFieldKey={true}
            ></EditableTable>
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
};

export default Comp;
