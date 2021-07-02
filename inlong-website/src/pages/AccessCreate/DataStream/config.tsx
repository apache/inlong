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

import React from 'react';
import { Divider } from 'antd';
import { genDataFields } from '@/components/AccessHelper';

export const genFormContent = (currentValues, bid) => {
  const extraParams = {
    businessIdentifier: bid,
  };

  return [
    {
      type: <Divider orientation="left">基础信息</Divider>,
    },
    ...genDataFields(
      ['dataStreamIdentifier', 'name', 'inCharges', 'description'],
      currentValues,
      extraParams,
    ),
    {
      type: <Divider orientation="left">数据来源</Divider>,
    },
    ...genDataFields(['dataSourceType', 'dataSourcesConfig'], currentValues, extraParams),
    {
      type: <Divider orientation="left">数据信息</Divider>,
    },
    ...genDataFields(
      ['dataType', 'dataEncoding', 'fileDelimiter', 'rowTypeFields'],
      currentValues,
      extraParams,
    ),
    {
      type: <Divider orientation="left">数据流向</Divider>,
    },
    ...genDataFields(['dataStorage', 'dataStorageHIVE'], currentValues, extraParams),
  ];
};
