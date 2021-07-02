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
import { Steps, Tooltip } from 'antd';

const { Step } = Steps;

export interface StepsProps {
  data: {
    title: string;
    desc: string;
    state: string;
    remark?: string;
  }[];
}

const Comp: React.FC<StepsProps> = ({ data }) => {
  const current = data.findIndex(item => item.state === 'PENDING');

  return (
    <Steps size="small" current={current !== -1 ? current : data.length} direction="vertical">
      {data.map((item, index) => (
        <Step
          key={index}
          title={
            item.remark ? (
              <Tooltip placement="bottom" title={item.remark}>
                {item.title}
              </Tooltip>
            ) : (
              item.title
            )
          }
          description={item.desc}
        />
      ))}
    </Steps>
  );
};

export default Comp;
