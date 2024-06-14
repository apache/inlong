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
import i18n from '@/i18n';
import {
  ApiOutlined,
  SettingOutlined,
  DatabaseOutlined,
  DeploymentUnitOutlined,
  SafetyOutlined,
  ShopOutlined,
  InteractionOutlined,
  ProfileOutlined,
} from '@ant-design/icons';
import type { MenuItemType } from '.';

const conf: MenuItemType[] = [
  {
    path: '/group',
    name: i18n.t('configs.menus.Groups'),
    icon: <ApiOutlined />,
  },
  {
    path: '/sync',
    name: i18n.t('configs.menus.DataSynchronize'),
    icon: <InteractionOutlined />,
  },
  {
    path: '/consume',
    name: i18n.t('configs.menus.Subscribe'),
    icon: <ShopOutlined />,
  },
  {
    path: '/node',
    name: i18n.t('configs.menus.Nodes'),
    icon: <DatabaseOutlined />,
  },
  {
    name: i18n.t('configs.menus.Clusters'),
    icon: <DeploymentUnitOutlined />,
    children: [
      {
        path: '/clusterTags',
        name: i18n.t('configs.menus.ClusterTags'),
      },
      {
        path: '/clusters',
        name: i18n.t('configs.menus.Clusters'),
      },
    ],
  },
  {
    path: '/process',
    name: i18n.t('configs.menus.Process'),
    icon: <SafetyOutlined />,
  },
  {
    name: i18n.t('configs.menus.SystemManagement'),
    icon: <SettingOutlined />,
    isAdmin: true,
    children: [
      {
        path: '/user',
        name: i18n.t('configs.menus.UserManagement'),
      },
      {
        path: '/tenant',
        isAdmin: true,
        name: i18n.t('configs.menus.TenantManagement'),
      },
      {
        path: '/approval',
        isAdmin: true,
        name: i18n.t('configs.menus.ProcessManagement'),
      },
      {
        path: '/dataTemplate',
        name: i18n.t('configs.menus.Groups.Template'),
      },
    ],
  },
  {
    name: i18n.t('configs.menus.SystemOperation'),
    icon: <ProfileOutlined />,
    isAdmin: true,
    children: [
      {
        path: '/system',
        name: i18n.t('configs.menus.ModuleAudit'),
      },
      {
        path: '/agentModule',
        name: i18n.t('configs.menus.agentModule'),
      },
      {
        path: '/agentPackage',
        name: i18n.t('configs.menus.agentPackage'),
      },
    ],
  },
];

export default conf;
