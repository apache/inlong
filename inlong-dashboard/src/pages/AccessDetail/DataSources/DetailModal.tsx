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
import { Modal } from 'antd';
import { ModalProps } from 'antd/es/modal';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import { useRequest, useUpdateEffect } from '@/hooks';
import { useTranslation } from 'react-i18next';
import { FormItemProps } from '@/components/FormGenerator';
import { sources, SourceType } from '@/metas/sources';

export interface Props extends ModalProps {
  type: 'MYSQL_BINLOG' | 'FILE';
  // When editing, use the ID to call the interface for obtaining details
  id?: string;
  // Pass when editing, directly echo the record data
  record?: Record<string, any>;
  // Additional form configuration
  content?: FormItemProps[];
}

const sourcesMap: Record<string, SourceType> = sources.reduce(
  (acc, cur) => ({
    ...acc,
    [cur.value]: cur,
  }),
  {},
);

const Comp: React.FC<Props> = ({ type, id, content = [], record, ...modalProps }) => {
  const [form] = useForm();
  const { t } = useTranslation();

  const [currentValues, setCurrentValues] = useState({});

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
        setCurrentValues(result);
      },
    },
  );

  const onOk = async () => {
    const values = await form.validateFields();
    if (data) values.version = data.version;
    modalProps?.onOk(toSubmitVals(values));
  };

  useUpdateEffect(() => {
    if (modalProps.visible) {
      // open
      form.resetFields(); // Note that it will cause the form to remount to initiate a select request
      if (id) {
        getData(id);
      } else if (!id && Object.keys(record || {})?.length) {
        form.setFieldsValue(toFormVals(record));
        setCurrentValues(toFormVals(record));
      }
    } else {
      setCurrentValues({});
    }
  }, [modalProps.visible]);

  const formContent = useMemo(() => {
    const getForm = sourcesMap[type].getForm;
    const config = getForm('form', {
      currentValues,
      form,
    }) as FormItemProps[];
    return [
      {
        name: 'sourceName',
        type: 'input',
        label: t('components.AccessHelper.DataSourcesEditor.CreateModal.DataSourceName'),
        rules: [{ required: true }],
        props: {
          disabled: !!id,
        },
      } as FormItemProps,
    ].concat(config);
  }, [type, id, currentValues, form, t]);

  return (
    <>
      <Modal {...modalProps} title={sourcesMap[type]?.label} width={666} onOk={onOk}>
        <FormGenerator
          content={content.concat(formContent)}
          onValuesChange={vals => setCurrentValues(prev => ({ ...prev, ...vals }))}
          allValues={currentValues}
          form={form}
          useMaxWidth
        />
      </Modal>
    </>
  );
};

export default Comp;
