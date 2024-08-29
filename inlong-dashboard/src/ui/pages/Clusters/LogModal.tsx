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
import { Modal, Spin, Input } from 'antd';
import { ModalProps } from 'antd/es/modal';
import { useRequest, useUpdateEffect } from '@/ui/hooks';
import i18n from '@/i18n';
import StatusTag from '@/ui/components/StatusTag';

export interface Props extends ModalProps {
  id?: string;
}

const Comp: React.FC<Props> = ({ id, ...modalProps }) => {
  const [log, setLog] = useState(null);

  const {
    data: logData,
    loading: testLoad1,
    run: getLog,
  } = useRequest(id => ({ url: `/cluster/node/get/${id}` }), {
    manual: true,
    onSuccess: result => {
      setLog(result.operateLog);
    },
  });

  useUpdateEffect(() => {
    if (modalProps.open) {
      if (id) {
        getLog(id);
      }
    }
  }, [modalProps.open]);

  return (
    <Modal
      {...modalProps}
      title={i18n.t('pages.Cluster.Node.InstallLog')}
      width={1200}
      footer={null}
    >
      <div style={{ marginTop: '20px' }}>
        <Spin spinning={testLoad1}>
          <div>
            <Input.TextArea
              rows={log === '' || log === null ? 0 : 24}
              value={
                log === '' || log === null ? i18n.t('pages.Cluster.Node.InstallLog.None') : log
              }
            />
          </div>
        </Spin>
      </div>
    </Modal>
  );
};

export default Comp;
