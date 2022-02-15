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

import React, { useMemo, useState } from 'react';
import { Modal } from 'antd';
import { ModalProps } from 'antd/es/modal';
import { useRequest, useUpdateEffect } from '@/hooks';
import FormGenerator, {
  useForm,
  FormItemProps,
  FormGeneratorProps,
} from '@/components/FormGenerator';
import { GetStorageFormFieldsType } from '@/utils/metaData';
import { getHiveForm, getHiveColumns } from '@/components/MetaData/StorageHive';
import { getClickhouseForm, getClickhouseColumns } from '@/components/MetaData/StorageClickhouse';

export interface DetailModalProps extends ModalProps {
  inlongGroupId: string;
  name?: string;
  content?: FormItemProps[];
  // (True operation, save and adjust interface) Need to upload when editing
  id?: string;
  // (False operation) Need to pass when editing, row data
  record?: Record<string, any>;
  // You can customize the conversion format after successfully obtaining the existing data during editing
  onSuccessDataFormat?: Function;
  storageType: 'HIVE' | 'TEST';
  dataType?: string;
  // defaultRowTypeFields, which can be used to auto-fill form default values
  defaultRowTypeFields?: Record<string, unknown>[];
  // others
  onOk?: (values) => void;
  onValuesChange?: FormGeneratorProps['onValuesChange'];
}

const Comp: React.FC<DetailModalProps> = ({
  inlongGroupId,
  id,
  record,
  storageType,
  onSuccessDataFormat,
  name,
  content = [],
  dataType,
  defaultRowTypeFields,
  onValuesChange,
  ...modalProps
}) => {
  const [form] = useForm();

  const [currentValues, setCurrentValues] = useState({});

  const fieldListKey = useMemo(() => {
    return {
      HIVE: {
        // Field name of the field array form
        columnsKey: 'fieldList',
        // Columns definition of field array
        getColumns: getHiveColumns,
        // In addition to the defaultRowTypeFields field that is populated by default, additional fields that need to be populated
        // The left is the defaultRowTypeFields field, and the right is the newly filled field
        restMapping: {
          fieldName: 'fieldName',
        },
      },
      CLICK_HOUSE: {
        columnsKey: 'fieldList',
        getColumns: getClickhouseColumns,
        restMapping: {
          fieldName: 'fieldName',
        },
      },
    }[storageType];
  }, [storageType]);

  const { data, run: getData } = useRequest(
    id => ({
      url: `/storage/get/${id}`,
      params: {
        storageType,
      },
    }),
    {
      manual: true,
      onSuccess: result => {
        const data =
          typeof onSuccessDataFormat === 'function' ? onSuccessDataFormat(result) : result;
        form.setFieldsValue(data);
        setCurrentValues(data);
        if (onValuesChange) {
          onValuesChange(data, data);
        }
      },
    },
  );

  useUpdateEffect(() => {
    if (modalProps.visible) {
      // open
      form.resetFields(); // Note that it will cause the form to remount to initiate a select request
      if (id) {
        getData(id);
        return;
      }
      if (Object.keys(record || {})?.length) {
        form.setFieldsValue(record);
        setCurrentValues(record);
      } else {
        const usefulDefaultRowTypeFields = defaultRowTypeFields?.filter(
          item => item.fieldName && item.fieldType,
        );
        if (fieldListKey && usefulDefaultRowTypeFields?.length) {
          form.setFieldsValue({
            [fieldListKey.columnsKey]: usefulDefaultRowTypeFields?.map(item => ({
              // The default value defined by cloumns
              ...fieldListKey.getColumns(dataType).reduce(
                (acc, cur) => ({
                  ...acc,
                  [cur.dataIndex]: cur.initialValue,
                }),
                {},
              ),
              // Extra fill
              ...Object.keys(fieldListKey.restMapping).reduce(
                (acc, key) => ({
                  ...acc,
                  [fieldListKey.restMapping[key]]: item[key],
                }),
                {},
              ),
              // Default fill
              sourceFieldName: item.fieldName,
              sourceFieldType: item.fieldType,
              fieldComment: item.fieldComment,
            })),
          });
        }
      }
    }
  }, [modalProps.visible]);

  const formContent = useMemo(() => {
    const map: Record<string, GetStorageFormFieldsType> = {
      HIVE: getHiveForm,
      CLICK_HOUSE: getClickhouseForm,
    };
    const item = map[storageType];

    return item('form', {
      dataType,
      isEdit: !!id,
      inlongGroupId,
      currentValues,
      form,
    }) as FormItemProps[];
  }, [storageType, dataType, inlongGroupId, id, currentValues, form]);

  const onOk = async () => {
    const values = await form.validateFields();
    values.filePath = (currentValues as any).filePath;
    modalProps.onOk && modalProps.onOk(values);
  };

  const onValuesChangeHandler = (...rest) => {
    setCurrentValues(rest[1]);

    if (onValuesChange) {
      (onValuesChange as any)(...rest);
    }
  };

  return (
    <Modal title={storageType} width={1200} {...modalProps} onOk={onOk}>
      <FormGenerator
        name={name}
        labelCol={{ span: 4 }}
        wrapperCol={{ span: 20 }}
        content={content.concat(formContent)}
        form={form}
        allValues={data}
        onValuesChange={onValuesChangeHandler}
      />
    </Modal>
  );
};

export default Comp;
