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

import React, { useEffect, useState, forwardRef, useImperativeHandle, useMemo } from 'react';
import FormGenerator, { useForm } from '@/components/FormGenerator';
import request from '@/utils/request';
import { State } from '@/models';
import { useSelector, useRequest } from '@/hooks';
import { getFormContent } from './config';

export interface Props {
  // If it has been created, save it again and use this value to update
  inlongGroupId?: string;
}

const Comp = ({ inlongGroupId }: Props, ref) => {
  const [form] = useForm();

  const { userName } = useSelector<State, State>(state => state);

  const [changedValues, setChangedValues] = useState<Record<string, any>>({});

  const isUpdate = useMemo(() => {
    return !!inlongGroupId;
  }, [inlongGroupId]);

  const { data: savedData } = useRequest(`/business/get/${inlongGroupId}`, {
    ready: !!inlongGroupId && !Object.keys(changedValues).length,
    refreshDeps: [inlongGroupId],
    formatResult: data => ({
      ...data,
      inCharges: data.inCharges.split(','),
      followers: data.inCharges.split(','),
    }),
    onSuccess: data => {
      form.setFieldsValue(data);
      setChangedValues(data);
    },
  });

  const onOk = async () => {
    const values = await form.validateFields();

    const data = {
      ...values,
      inCharges: values.inCharges?.join(','),
      followers: values.followers?.join(','),
      mqExtInfo: {
        ...values.mqExtInfo,
        middlewareType: values.middlewareType,
      },
    };

    if (isUpdate) {
      data.inlongGroupId = inlongGroupId;
      if (changedValues.mqExtInfo?.id) data.mqExtInfo.id = changedValues.mqExtInfo.id;
    }

    const result = await request({
      url: isUpdate ? '/business/update' : '/business/save',
      method: 'POST',
      data,
    });
    return {
      ...values,
      inlongGroupId: result,
    };
  };

  useEffect(() => {
    const values = {} as Record<string, unknown>;
    if (!isUpdate) {
      if (userName) values.inCharges = [userName];
      form.setFieldsValue(values);
      setChangedValues(values);
    }
  }, [isUpdate, form, userName]);

  useImperativeHandle(ref, () => ({
    onOk,
  }));

  return (
    <>
      <FormGenerator
        form={form}
        content={getFormContent({ changedValues, isUpdate })}
        onValuesChange={(c, v) => setChangedValues(v)}
        useMaxWidth={800}
        allValues={savedData}
      />
    </>
  );
};

export default forwardRef(Comp);
