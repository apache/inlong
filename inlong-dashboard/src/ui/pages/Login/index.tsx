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
import { Button } from 'antd';
import { UserOutlined, LockOutlined } from '@ant-design/icons';
import { useTranslation } from 'react-i18next';
import { config } from '@/configs/default';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import request from '@/core/utils/request';
import styles from './index.module.less';

const Comp: React.FC = () => {
  const { t } = useTranslation();
  const [form] = useForm();

  const formConfig = [
    {
      type: 'input',
      name: 'username',
      props: {
        placeholder: t('pages.Login.PleaseEnterUserName'),
        size: 'large',
        prefix: <UserOutlined />,
      },
      rules: [
        { required: true, message: t('pages.Login.PleaseEnterUserName') },
        { pattern: /^\S+$/, message: t('pages.Login.UserNameSpaceNotAllowed') },
      ],
    },
    {
      type: 'password',
      name: 'password',
      props: {
        placeholder: t('pages.Login.PleaseEnterYourPassword'),
        size: 'large',
        prefix: <LockOutlined />,
      },
      rules: [
        { required: true, message: t('pages.Login.PleaseEnterYourPassword') },
        { pattern: /^\S+$/, message: t('pages.Login.UserPasswordSpaceNotAllowed') },
      ],
    },
  ];

  const login = async () => {
    const data = await form.validateFields();
    // The backend now returns { success, username, userId, mustChangePassword }.
    // Old clients used to treat `data` as a boolean; the truthy object value
    // keeps the legacy `if (result)` checks working.
    const result = await request({
      url: '/anno/login',
      method: 'POST',
      data,
    });
    if (result && result.mustChangePassword) {
      // Hand the flag over to the dashboard shell. Using sessionStorage (not
      // localStorage) so the prompt clears as soon as the tab is closed; the
      // backend interceptor still blocks every API call until the password is
      // actually rotated, so there is no risk of bypass.
      sessionStorage.setItem('inlong.mustChangePassword', '1');
      if (result.userId != null) {
        sessionStorage.setItem('inlong.mustChangePasswordUserId', String(result.userId));
      }
    } else {
      sessionStorage.removeItem('inlong.mustChangePassword');
      sessionStorage.removeItem('inlong.mustChangePasswordUserId');
    }
    window.location.href = '/';
  };

  const onEnter = e => {
    if (e.keyCode === 13) login();
  };

  return (
    <div className={styles.containerBg} onKeyUp={onEnter}>
      <div className={styles.container}>
        <img src={config.logo} style={{ width: '100%' }} alt={config.title} />
        <FormGenerator form={form} content={formConfig} />
        <Button type="primary" onClick={login} style={{ width: '100%' }} size="large">
          {t('pages.Login.Login')}
        </Button>
      </div>
    </div>
  );
};

export default Comp;
