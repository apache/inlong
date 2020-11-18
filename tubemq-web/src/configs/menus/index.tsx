import React from 'react';
import { Route } from '@/typings/router';
import {
  NodeExpandOutlined,
  ClusterOutlined,
  SettingOutlined,
} from '@ant-design/icons';
/*
 * Note:
 * Menu items with children need to set a key starting with "/"
 * @see https://github.com/umijs/route-utils/blob/master/src/transformRoute/transformRoute.ts#L219
 */

const menus: Route[] = [
  {
    path: '/issue',
    name: '分发查询',
    icon: <NodeExpandOutlined />,
    hideChildrenInMenu: true,
    children: [
      {
        path: '/:id',
        name: '消费组详情',
      },
    ],
  },
  {
    name: '配置管理',
    key: '/other',
    icon: <SettingOutlined />,
    path: '/other',
    children: [
      {
        path: '/broker',
        name: 'Broker列表',
      },
      {
        path: '/topic',
        name: 'topic列表',
      },
    ],
  },
  {
    path: '/cluster',
    name: '集群管理',
    icon: <ClusterOutlined />,
  },
];

export default menus;
