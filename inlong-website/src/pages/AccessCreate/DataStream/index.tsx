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

import React, { useState, useEffect, useImperativeHandle, forwardRef } from 'react';
import { Form, Collapse, Button, Space } from 'antd';
import { useTranslation } from 'react-i18next';
import { FormItemContent } from '@/components/FormGenerator';
import { useRequest, useSelector } from '@/hooks';
import request from '@/utils/request';
import { State } from '@/models';
import { genFormContent } from './config';
import { valuesToData, dataToValues } from './helper';

export interface Props {
  bid: string;
}

const Comp = ({ bid }: Props, ref) => {
  const [form] = Form.useForm();

  const { t } = useTranslation();

  const userName = useSelector<State, State['userName']>(state => state.userName);

  const [activeKey, setActiveKey] = useState('0');

  const [realTimeValues, setRealTimeValues] = useState({ list: [] });

  useRequest(
    {
      url: '/datastream/listAll',
      params: {
        pageSize: 100,
        pageNum: 1,
        bid,
      },
    },
    {
      formatResult: result => ({
        list: dataToValues(result.list),
        total: result.total,
      }),
      onSuccess: data => {
        if (data.list?.length) {
          form.setFieldsValue({ list: data.list });
          setRealTimeValues({ list: data.list });
        }
      },
    },
  );

  useEffect(() => {
    if (form) {
      setTimeout(() => {
        const { getFieldsValue } = form;
        const values = getFieldsValue(true);
        setRealTimeValues(values);
      }, 0);
    }
  }, [form]);

  const onOk = async () => {
    const { list } = await form.validateFields();
    const data = valuesToData(list, bid);
    await request({
      url: '/datastream/batchSaveAll',
      method: 'POST',
      data,
    });
    const result = await request({
      url: `/business/startProcess/${bid}`,
      method: 'POST',
    });
    return result;
  };

  useImperativeHandle(ref, () => ({
    onOk,
  }));

  const genExtra = (field, removeFunc, index, arrLen) =>
    !(index === 0 && arrLen === 1) && (
      <Button size="small" type="link" onClick={() => removeFunc(field.name)}>
        {t('basic.Delete')}
      </Button>
    );

  return (
    userName && (
      <Form
        form={form}
        name="dataSourcesForm"
        labelAlign="left"
        labelCol={{ xs: 6, sm: 4 }}
        wrapperCol={{ xs: 18, sm: 20 }}
        onValuesChange={(c, v) => setRealTimeValues(v)}
      >
        <Form.List name="list" initialValue={[{}]}>
          {(fields, { add, remove }) => (
            <>
              <Space style={{ marginBottom: 20 }}>
                <Button
                  type="primary"
                  onClick={async () => {
                    await add();
                    setActiveKey(fields.length.toString());
                  }}
                >
                  {t('basic.Create')}
                </Button>
              </Space>

              <Collapse
                accordion
                activeKey={activeKey}
                onChange={key => setActiveKey(key as any)}
                style={{ backgroundColor: 'transparent', border: 'none' }}
              >
                {fields.map((field, index, arr) => (
                  <Collapse.Panel
                    header={`#${index + 1}`}
                    key={index.toString()}
                    extra={genExtra(field, remove, index, arr.length)}
                    style={{
                      marginBottom: 10,
                      border: '1px solid #e5e5e5',
                      backgroundColor: '#f6f7fb',
                    }}
                  >
                    <FormItemContent
                      values={realTimeValues.list?.[index]}
                      content={genFormContent(
                        {
                          ...realTimeValues.list?.[index],
                          inCharges: [userName],
                        },
                        bid,
                      ).map(item => {
                        const obj = { ...item } as any;
                        if (obj.name) {
                          obj.name = [field.name, obj.name];
                          obj.fieldKey = [field.fieldKey, obj.name];
                        }
                        if (obj.suffix && obj.suffix.name) {
                          obj.suffix.name = [field.name, obj.suffix.name];
                          obj.suffix.fieldKey = [field.fieldKey, obj.suffix.name];
                        }
                        return obj;
                      })}
                    />
                  </Collapse.Panel>
                ))}
              </Collapse>
            </>
          )}
        </Form.List>
      </Form>
    )
  );
};

export default forwardRef(Comp);
