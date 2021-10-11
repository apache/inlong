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

/**
 * A select that can automatically initiate asynchronous (cooperating with useRequest) to obtain drop-down list data
 */
import React, { useMemo } from 'react';
import { Select } from 'antd';
import type { SelectProps, OptionProps } from 'antd/es/select';
import { useRequest } from '@/hooks';

// example options: {
//   requestService: '/basic/schema/listAll',
//   requestParams: {
//     formatResult: result =>
//       result.map(item => ({
//         label: item.name,
//         value: item.name,
//       })),
//   },
//   requestAuto: false // Whether to automatically initiate a request when the component is mounted, by default when the drop-down box is expanded
// },
export interface HighSelectProps extends Omit<SelectProps<any>, 'options'> {
  options?:
    | OptionProps
    | {
        requestService?: unknown;
        requestParams?: unknown;
        requestAuto?: boolean;
      };
  asyncValueLabel?: string;
}

const HighSelect: React.FC<HighSelectProps> = ({ options, asyncValueLabel, ...rest }) => {
  const { data: list = [], run: getList } = useRequest(options?.requestService, {
    manual: !options?.requestAuto,
    ready: !!options?.requestService && (options?.requestParams?.ready ?? true),
    ...options?.requestParams,
  });

  const optionList = useMemo(() => {
    if (Array.isArray(options)) {
      return options;
    }

    return list;
  }, [list, options]);

  const onDropdownVisibleChange = (open: boolean) => {
    if (open) {
      getList();
    }
    if (rest.onDropdownVisibleChange) {
      rest.onDropdownVisibleChange(open);
    }
  };

  const onChange = value => {
    const optionItem = optionList.find(item => item.value === value);
    if (typeof rest.onChange === 'function') {
      rest.onChange(value, optionItem);
    }
  };

  return (
    <Select
      showSearch={optionList.length > 5}
      {...rest}
      onDropdownVisibleChange={onDropdownVisibleChange}
      onChange={onChange}
      value={(!optionList.length && asyncValueLabel) || rest.value}
      options={optionList.map(item => ({
        label: item.label,
        value: item.value,
        title: item.title,
        disabled: item.disabled,
      }))}
    />
  );
};

export default HighSelect;
