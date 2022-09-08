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

import React, { useCallback, useMemo, useState } from 'react';
import { Modal, message } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/hooks';
import { useTranslation } from 'react-i18next';
import { FormItemProps } from '@/components/FormGenerator';
import { sources } from '@/metas/sources';
import request from '@/utils/request';

export interface Props extends ModalProps {
  // When editing, use the ID to call the interface for obtaining details
  id?: string;
  inlongGroupId?: string;
}

const sourcesMap: Record<string, typeof sources[0]> = sources.reduce(
  (acc, cur) => ({
    ...acc,
    [cur.value]: cur,
  }),
  {},
);

const Comp: React.FC<Props> = ({ id, inlongGroupId, ...modalProps }) => {
  const [form] = useForm();
  const { t } = useTranslation();

  const [type, setType] = useState(sources[0].value);

  const toFormVals = useCallback(
    v => {
      const mapFunc = sourcesMap[type]?.toFormValues;
      return mapFunc ? mapFunc(v) : v;
    },
    [type],
  );

  const toSubmitVals = useCallback(
    v => {
      const mapFunc = sourcesMap[type]?.toSubmitValues;
      return mapFunc ? mapFunc(v) : v;
    },
    [type],
  );

  const { data, run: getData } = useRequest(
    id => ({
      url: `/source/get/${id}`,
      params: {
        sourceType: type,
      },
    }),
    {
      manual: true,
      formatResult: result => toFormVals(result),
      onSuccess: result => {
        form.setFieldsValue(result);
        setType(result.sourceType);
      },
    },
  );

  const onOk = async () => {
    const values = await form.validateFields();
    const submitData = toSubmitVals(values);
    const isUpdate = Boolean(id);
    if (isUpdate) {
      submitData.id = id;
      submitData.version = data?.version;
    }
    await request({
      url: `/source/${isUpdate ? 'update' : 'save'}`,
      method: 'POST',
      data: {
        ...submitData,
        inlongGroupId,
      },
    });
    modalProps?.onOk(submitData);
    message.success(t('pages.GroupDetail.Sources.SaveSuccessfully'));
  };

  useUpdateEffect(() => {
    if (modalProps.visible) {
      // open
      if (id) {
        getData(id);
      }
    } else {
      form.resetFields();
      setType(sources[0].value);
    }
  }, [modalProps.visible]);

  const formContent = useMemo(() => {
    const currentForm = sourcesMap[type]?.form;
    return [
      {
        type: 'select',
        label: t('pages.GroupDetail.Sources.DataStreams'),
        name: 'inlongStreamId',
        props: {
          disabled: !!id,
          options: {
            requestService: {
              url: '/stream/list',
              method: 'POST',
              data: {
                pageNum: 1,
                pageSize: 1000,
                inlongGroupId,
              },
            },
            requestParams: {
              formatResult: result =>
                result?.list.map(item => ({
                  label: item.inlongStreamId,
                  value: item.inlongStreamId,
                })) || [],
            },
          },
        },
        rules: [{ required: true }],
      } as FormItemProps,
    ].concat(currentForm);
  }, [type, id, t, inlongGroupId]);

  return (
    <>
      <Modal {...modalProps} title={sourcesMap[type]?.label} width={666} onOk={onOk}>
        <FormGenerator
          content={formContent}
          onValuesChange={(c, values) => setType(values.sourceType)}
          initialValues={id ? data : {}}
          form={form}
          useMaxWidth
        />
      </Modal>
    </>
  );
};

export default Comp;
