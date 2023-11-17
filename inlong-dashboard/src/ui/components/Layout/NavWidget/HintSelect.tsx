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
import { Dropdown, Menu } from 'antd';
import { QuestionCircleOutlined } from '@ant-design/icons';
import { localesConfig } from '@/configs/locales';
import i18n from '@/i18n';

const HintSelect = () => {
  const langMenu = (
    <Menu>
      <Menu.Item>
        <a target="_blank" href="https://inlong.apache.org/docs/next/introduction">
          {i18n.t('components.Layout.UserManual')}
        </a>
      </Menu.Item>
      <Menu.Item>
        <a target="_blank" href="https://github.com/apache/inlong/discussions">
          {i18n.t('components.Layout.Feedback')}
        </a>
      </Menu.Item>
    </Menu>
  );

  return (
    <Dropdown dropdownRender={() => langMenu} placement="bottomRight">
      <QuestionCircleOutlined />
    </Dropdown>
  );
};

export default HintSelect;
