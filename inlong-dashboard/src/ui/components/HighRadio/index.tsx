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
import React, { useEffect, useMemo, useState } from 'react';
import { Input, Radio, RadioGroupProps } from 'antd';
import i18n from '@/i18n';

interface optionProps {
  label: string;
  value: number;
}
export interface HighRadioProps extends Omit<RadioGroupProps, 'options'> {
  options: optionProps[];
  useInput?: boolean;
  useInputProps?: Record<string, unknown>;
}

const HighRadio: React.FC<HighRadioProps> = ({
  options,
  useInput = false,
  useInputProps,
  ...rest
}) => {
  const [diyWatcher, setDiyWatcher] = useState(true);
  const [diyState, setDiyState] = useState(false);
  const optionList = useMemo(() => {
    return useInput
      ? [
          ...options,
          {
            label: i18n.t('components.HighRadio.Customize'),
            value: '__DIYState',
          },
        ]
      : options;
  }, [options, useInput]);

  useEffect(() => {
    if (diyWatcher && optionList.every(item => item.value !== rest.value) && !diyState) {
      setDiyState(true);
    }
  }, [diyWatcher, rest.value, optionList, diyState]);

  const onValueChange = value => {
    if (typeof rest.onChange === 'function') {
      rest.onChange(value);
    }
  };

  const onRadioChange = e => {
    const newDiyState = e.target.value === '__DIYState';
    if (diyState !== newDiyState) {
      setDiyState(newDiyState);
    }
    if (newDiyState) {
      setDiyWatcher(false);
      return;
    }
    onValueChange(e);
  };
  const RadioComponent = (
    <Radio.Group
      {...rest}
      options={optionList}
      onChange={onRadioChange}
      value={useInput && diyState ? '__DIYState' : (!optionList.length && rest.value) || rest.value}
    />
  );
  const onInputChange = e => {
    onValueChange(e.target.value);
  };
  return useInput ? (
    <div
      style={{
        display: 'flex-inline',
        flexWrap: 'wrap',
        height: 50,
        position: 'relative',
      }}
    >
      {RadioComponent}
      {useInput && diyState && (
        <Input {...useInputProps} value={rest.value} onChange={onInputChange} />
      )}
    </div>
  ) : (
    RadioComponent
  );
};

export default HighRadio;
