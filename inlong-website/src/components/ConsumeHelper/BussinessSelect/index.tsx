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

import React, { useState, useEffect } from 'react';
import { Button, Input, Space } from 'antd';
import type { InputProps } from 'antd/es/input';
import MyBussinessModal from './MyBussinessModal';

export interface Props extends Omit<InputProps, 'onChange'> {
  value?: string;
  onChange?: (value: string) => void;
  onSelect?: (value: Record<string, any>) => void;
}

const Comp: React.FC<Props> = ({ value, onChange, onSelect, ...rest }) => {
  const [data, setData] = useState(value);

  const [myBussinessModal, setMyBussinessModal] = useState({
    visible: false,
  });

  useEffect(() => {
    if (value !== data) {
      setData(value);
    }
    // eslint-disable-next-line
  }, [value]);

  const triggerChange = newData => {
    if (onChange) {
      onChange(newData);
    }
  };

  const onSelectRow = rowValues => {
    setData(rowValues);
    triggerChange(rowValues);
  };

  const onTextChange = value => {
    setData(value);
    triggerChange(value);
  };

  return (
    <>
      <Space>
        <Input value={data} onChange={e => onTextChange(e.target.value)} {...rest} />
        <Button type="link" onClick={() => setMyBussinessModal({ visible: true })}>
          查询
        </Button>
      </Space>

      <MyBussinessModal
        {...myBussinessModal}
        visible={myBussinessModal.visible}
        onOk={(value, record) => {
          onSelectRow(value);
          if (onSelect) {
            onSelect(record);
          }
          setMyBussinessModal({ visible: false });
        }}
        onCancel={() => setMyBussinessModal({ visible: false })}
      />
    </>
  );
};

export default Comp;
