/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { useEffect, useState } from 'react';
import i18n from '@/i18n';
import { Alert, Button, Modal, message } from 'antd';
import { useTranslation } from 'react-i18next';
import FormGenerator, { useForm } from '@/ui/components/FormGenerator';
import request from '@/core/utils/request';

/**
 * Force password change modal.
 *
 * Trigger: the login page sets the sessionStorage flag `inlong.mustChangePassword=1`,
 * Layout detects this flag after mounting and opens this modal.
 *
 * Behavioral constraints:
 *  - Non-closable: no top-right X, ESC disabled, mask-click disabled, footer is a single "Submit" button;
 *  - On success: clears sessionStorage flags, auto-logouts, redirects to login page;
 *  - If the user closes the browser without changing the password, they can still log in
 *    with the default password next time and will be intercepted again;
 *    during this period, business APIs are blocked by the backend interceptor (403),
 *    leaving no bypass risk.
 */
const SS_FLAG_KEY = 'inlong.mustChangePassword';
const SS_USER_ID_KEY = 'inlong.mustChangePasswordUserId';

const ForcePasswordChangeModal: React.FC = () => {
  const { t } = useTranslation();
  const [form] = useForm();
  const [open, setOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [userId, setUserId] = useState<number | null>(null);
  const [userMeta, setUserMeta] = useState<{
    name?: string;
    version?: number;
    accountType?: number;
    validDays?: number;
  }>({});

  // 1) Check sessionStorage flag
  useEffect(() => {
    if (sessionStorage.getItem(SS_FLAG_KEY) === '1') {
      setOpen(true);
      const cached = sessionStorage.getItem(SS_USER_ID_KEY);
      if (cached) {
        setUserId(Number(cached));
      }
    }
  }, []);

  // 2) Fetch current user info (version/accountType/validDays) required by password update API
  useEffect(() => {
    if (!open) {
      return;
    }
    (async () => {
      try {
        const me: any = await request({ url: '/user/currentUser', method: 'POST' });
        const resolvedId = userId ?? me?.id;
        setUserId(resolvedId);
        if (resolvedId != null) {
          const detail: any = await request({ url: `/user/get/${resolvedId}` });
          setUserMeta({
            name: detail?.name,
            version: detail?.version,
            accountType: detail?.accountType,
            validDays: detail?.validDays,
          });
        }
      } catch (err) {
        // failing to fetch user info is harmless — /user/update validates on its own
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const formContent = [
    {
      type: 'password',
      label: t('components.Layout.NavWidget.Password'),
      name: 'password',
      props: { autoFocus: true },
      rules: [{ required: true }],
    },
    {
      type: 'password',
      label: t('components.Layout.NavWidget.NewPassword'),
      name: 'newPassword',
      rules: [
        { required: true },
        { pattern: /^[@0-9a-zA-Z_-]+$/, message: t('pages.Login.PasswordRules') },
        ({ getFieldValue }) => ({
          validator(_, val) {
            if (!val) return Promise.resolve();
            if (val === getFieldValue('password')) {
              return Promise.reject(
                new Error(
                  // reuse existing i18n key for "new password must differ from old"
                  t('components.Layout.NavWidget.Remind'),
                ),
              );
            }
            return Promise.resolve();
          },
        }),
      ],
    },
    {
      type: 'password',
      label: t('components.Layout.NavWidget.ConfirmPassword'),
      name: 'confirmPassword',
      rules: [
        { required: true },
        ({ getFieldValue }) => ({
          validator(_, val) {
            if (!val) return Promise.resolve();
            return val === getFieldValue('newPassword')
              ? Promise.resolve()
              : Promise.reject(new Error(t('components.Layout.NavWidget.Remind')));
          },
        }),
      ],
    },
  ];

  const onSubmit = async () => {
    try {
      const values = await form.validateFields();
      setSubmitting(true);
      const payload = {
        id: userId,
        name: userMeta.name,
        version: userMeta.version,
        accountType: userMeta.accountType,
        validDays: userMeta.validDays,
        password: values.password,
        newPassword: values.newPassword,
      };
      await request({ url: '/user/update', method: 'POST', data: payload });
      // clear flags to avoid false re-trigger on next default-password login
      sessionStorage.removeItem(SS_FLAG_KEY);
      sessionStorage.removeItem(SS_USER_ID_KEY);
      message.success(t('basic.OperatingSuccess'));
      // force logout after password change so the user logs in with the new password
      try {
        await request({ url: '/anno/logout' });
      } catch (_) {
        // logout failure should not block the redirect
      }
      window.location.href = `${window.location.origin}/#/${i18n?.language || ''}/login`;
    } catch (err) {
      // validation / API errors are already surfaced by the global request interceptor
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      title={t('components.Layout.NavWidget.EditPassword')}
      width={520}
      closable={false}
      maskClosable={false}
      keyboard={false}
      destroyOnClose
      footer={[
        <Button key="submit" type="primary" loading={submitting} onClick={onSubmit}>
          {t('basic.Save')}
        </Button>,
      ]}
    >
      <Alert
        type="warning"
        showIcon
        style={{ marginBottom: 16 }}
        message={t(
          // i18n fallback: if the resource file is missing this key the text below is still readable
          'components.Layout.NavWidget.MustChangeDefaultPassword',
          'For security, you must change the default password before continuing.',
        )}
      />
      <FormGenerator content={formContent} form={form} useMaxWidth />
    </Modal>
  );
};

export default ForcePasswordChangeModal;
