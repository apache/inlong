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

import React, { useState } from 'react';
import { Button, Divider, Dropdown, Input, MenuProps, message, Select, Space, theme } from 'antd';
import { useRequest, useSelector } from '@/ui/hooks';
import { State } from '@/core/stores';
import { useTranslation } from 'react-i18next';
import request from '@/core/utils/request';
import KeyModal from '../NavWidget/KeyModal';
import { useLocalStorage } from '@/core/utils/localStorage';

const { useToken } = theme;

const Comp: React.FC = () => {
  const { t } = useTranslation();
  const tenant = useSelector<State, State['tenant']>(state => state.tenant);
  const userName = useSelector<State, State['userName']>(state => state.userName);
  const roles = useSelector<State, State['roles']>(state => state.roles);

  const [createModal, setCreateModal] = useState<Record<string, unknown>>({
    open: false,
  });

  const [keyModal, setKeyModal] = useState<Record<string, unknown>>({
    open: false,
  });

  const defaultOptions = {
    keyword: '',
    pageSize: 10,
    pageNum: 1,
  };

  const [options, setOptions] = useState(defaultOptions);

  const { data: savedData, run: getStreamData } = useRequest(
    {
      url: '/role/tenant/list',
      method: 'POST',
      data: {
        ...options,
        username: userName,
      },
    },
    { refreshDeps: [options] },
  );

  const onOk = async () => {
    await request({
      url: '/role/tenant/save',
      method: 'POST',
      data: {
        roleCode: 'TENANT_ADMIN',
        tenant: 'tenant2',
        username: 'admin',
      },
    });
  };

  const [getLocalStorage, setLocalStorage, removeLocalStorage] = useLocalStorage('tenant');

  const runLogout = async () => {
    await request('/anno/logout');
    window.location.href = '/';
  };
  const tenantData = { list: [] };

  const testClick = () => {
    console.log(12);
    setLocalStorage({ name: 'testTTTTTT' });
  };

  // for (let i = 0; i < 5; i++) {
  //   const t = {
  //     label: 'tenant_test_' + i,
  //     value: 'tenant_test_' + i,
  //     onClick: testClick(),
  //   };
  //   tenantData.list.push(t);
  // }
  // console.log(savedData, 'savedData');
  // console.log(roles, 'roles');
  const menuItems = [
    {
      label: 'tenant1',
      key: 'tenant1',
    },
    {
      label: 'tenant2',
      key: 'tenant2',
    },
    {
      label: 'tenant3',
      key: 'tenant3',
    },
    {
      label: 'tenant4',
      key: 'tenant4',
    },
  ];

  const { token } = useToken();

  const contentStyle = {
    backgroundColor: token.colorBgElevated,
    borderRadius: token.borderRadiusLG,
    boxShadow: token.boxShadowSecondary,
  };

  const menuStyle = {
    boxShadow: 'none',
  };

  const onClick: MenuProps['onClick'] = ({ key }) => {
    console.log(key, 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk');
    setLocalStorage({ name: key });
    window.location.reload();
  };

  const onFilter = allValues => {
    console.log(allValues, 'allValues');
  };

  return (
    <>
      <Dropdown
        menu={{ items: menuItems, onClick }}
        // menu={{ items: tenantData.list }}
        placement="bottomLeft"
        dropdownRender={menu => (
          <div style={contentStyle}>
            {React.cloneElement(menu as React.ReactElement, { style: menuStyle })}
            <Divider style={{ margin: 0 }} />
            <Space style={{ padding: 8 }}>
              <Input.Search
                // placeholder={t('请输入租户名称')}
                allowClear
                onSearch={onFilter}
                style={{ width: 130 }}
              />
              {/* <Button type="primary" onClick={onOk}>
                Click me!
              </Button> */}
            </Space>
          </div>
        )}
      >
        <span style={{ fontSize: 14 }}>{tenant}</span>
      </Dropdown>
      {/* <Select style={{ width: 100 }} /> */}
      <KeyModal
        {...keyModal}
        open={keyModal.open as boolean}
        onCancel={() => setKeyModal({ open: false })}
        onOk={async () => {
          setKeyModal({ open: false });
        }}
      />
    </>
  );
};

export default Comp;
