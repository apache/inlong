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
import request, { extendRequest } from '@/core/utils/request';
import { useLocalStorage } from '@/core/utils/localStorage';

const { useToken } = theme;

const Comp: React.FC = () => {
  const { t } = useTranslation();
  const tenant = useSelector<State, State['tenant']>(state => state.tenant);
  const userName = useSelector<State, State['userName']>(state => state.userName);
  const [getLocalStorage, setLocalStorage, removeLocalStorage] = useLocalStorage('tenant');

  const { token } = useToken();
  const contentStyle = {
    backgroundColor: token.colorBgElevated,
    borderRadius: token.borderRadiusLG,
    boxShadow: token.boxShadowSecondary,
  };
  const [filter, setFilter] = useState(true);
  const [filterData, setFilterData] = useState([]);
  const [data] = useState([]);

  const defaultOptions = {
    keyword: tenant,
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
    {
      refreshDeps: [options],
      onSuccess: result => {
        result.list.map(item => {
          return data.push({
            label: item.tenant,
            key: item.tenant,
          });
        });
      },
    },
  );

  const menuItems = [
    {
      label: 'tenant1',
      key: 'tenant11',
    },
    {
      label: 'tenant2',
      key: 'tenant22',
    },
    {
      label: 'tenant3',
      key: 'tenant33',
    },
    {
      label: 'tenant4',
      key: 'tenant44',
    },
  ];

  const { run: getData } = useRequest(
    name => ({
      url: `/user/currentUser`,
      method: 'post',
      headers: {
        rname: 'tenant',
        tenant: name,
      },
    }),
    {
      manual: true,
    },
  );

  const onClick: MenuProps['onClick'] = ({ key }) => {
    setLocalStorage({ name: key });
    getData(key);
    // extendRequest.interceptors.request.use((url, options) => {
    //   return {
    //     options: {
    //       ...options,
    //       interceptors: true,
    //       headers: { rname: 'tenant', value: key },
    //     },
    //   };
    // });
    message.success(t('切换成功'));
    window.location.reload();
  };

  const onFilter = allValues => {
    setFilterData(data.filter(item => item.key === allValues));
    setFilter(false);
  };

  return (
    <>
      <Dropdown
        menu={{ items: filter ? data : filterData, onClick }}
        placement="bottomLeft"
        dropdownRender={menu => (
          <div style={contentStyle}>
            {React.cloneElement(menu as React.ReactElement, { style: { boxShadow: 'none' } })}
            <Divider style={{ margin: 0 }} />
            <Space style={{ padding: 8 }}>
              <Input.Search allowClear onSearch={onFilter} style={{ width: 130 }} />
            </Space>
          </div>
        )}
      >
        <span style={{ fontSize: 14 }}>{tenant}</span>
      </Dropdown>
    </>
  );
};

export default Comp;
