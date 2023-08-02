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

import React, { useMemo, forwardRef, useImperativeHandle, useEffect } from 'react';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import { CommonInterface } from './common';
import { useGroupFormContent, getFormContent } from './GroupConfig';
import { useLocation } from '@/ui/hooks';
import { parse } from 'qs';
import { getSyncFormContent, useSyncFormContent } from './SyncConfig';

export type Props = CommonInterface;

const Comp = ({ defaultData, isViwer, suffixContent, noExtraForm, isFinished }: Props, ref) => {
  const [form] = useForm();

  const location = useLocation();

  const inlongGroupMode = useMemo(
    () => parse(location.search.slice(1))?.inlongGroupMode,
    [location.search],
  );

  const onOk = async (useValidate = true) => {
    if (!useValidate) {
      const values = await form.getFieldsValue();
      return values;
    }
    const values = await form.validateFields();

    const data = {
      groupApproveInfo: values,
    };

    return { ...values, form: data };
  };

  useEffect(() => {
    const groupInfo = defaultData?.processInfo?.formData?.groupInfo;
    const groupApproveInfo = defaultData?.currentTask?.formData?.groupApproveInfo;
    const streamInfo = defaultData?.processInfo?.formData?.streamInfoList[0];
    if (groupInfo || groupApproveInfo) {
      const obj = {
        ...groupInfo,
        ...groupApproveInfo,
      };
      if (inlongGroupMode === '1') {
        obj.inlongStreamId = streamInfo.inlongStreamId;
        obj.streamName = streamInfo.name;
      }
      form.setFieldsValue(obj);
    }
  }, [defaultData, form]);

  useImperativeHandle(ref, () => ({
    onOk,
  }));

  const groupFormContent = useGroupFormContent({
    mqType: defaultData?.processInfo?.formData?.groupInfo?.mqType,
    isViwer,
    isFinished,
  });

  const syncFormContent = useSyncFormContent({
    mqType: defaultData?.processInfo?.formData?.groupInfo?.mqType,
  });

  const processContent =
    inlongGroupMode === '1'
      ? getSyncFormContent({
          isViwer,
          suffixContent,
          syncFormContent,
        })
      : getFormContent({
          isViwer,
          formData: defaultData?.processInfo?.formData,
          suffixContent,
          noExtraForm,
          isFinished,
          groupFormContent,
        });

  // Easy to set some default values of the form
  const dataLoaded = useMemo(() => {
    return !!(defaultData && Object.keys(defaultData).length);
  }, [defaultData]);

  return dataLoaded && <FormGenerator form={form} content={processContent} />;
};

export default forwardRef(Comp);
