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
import { useRequest } from '@/ui/hooks';
import i18n from '@/i18n';
import { useForm } from '@/ui/components/HighTable';
import FormGenerator from '@/ui/components/FormGenerator';
import { Modal, Table } from 'antd';

export interface RowType {
  fieldName: string;
  fieldType: string;
  fieldComment: string;
}

interface SelectTemplateModalProps {
  visible: boolean;
  onHide: () => void;
  onClearAppend: (fields: RowType[]) => void;
}

const SelectTemplateModal: React.FC<SelectTemplateModalProps> = ({
  visible,
  onHide,
  onClearAppend,
}) => {
  const [form] = useForm();
  const handleCancel = () => {
    onHide();
  };
  const { data, run: getData } = useRequest(
    template => ({
      url: '/template/get',
      params: {
        templateName: template,
      },
    }),
    {
      manual: true,
    },
  );

  const columns = [
    {
      title: 'Name',
      dataIndex: 'fieldName',
      key: 'fieldName',
    },
    {
      title: 'Type',
      dataIndex: 'fieldType',
      key: 'fieldType',
    },
    {
      title: 'Field comment',
      dataIndex: 'fieldComment',
      key: 'fieldComment',
    },
  ];
  const getFilterFormContent = () => {
    return [
      {
        label: i18n.t('components.SelectTemplateModal.TemplateSelect'),
        tooltip: i18n.t('components.SelectTemplateModal.SelectTooltip'),
        name: 'streamDataTemplate',
        type: 'select',
        visible: values => {
          return !Boolean(values.id);
        },
        props: values => ({
          showSearch: true,
          optionFilterProp: 'label',
          allowClear: true,
          options: {
            requestTrigger: ['onOpen'],
            requestService: keyword => ({
              url: '/template/list',
              method: 'POST',
              data: {
                keyword,
                pageNum: 1,
                pageSize: 20,
              },
            }),
            requestParams: {
              formatResult: result => {
                return result?.list?.map(item => ({
                  ...item,
                  label: item.name,
                  value: item.name,
                }));
              },
            },
          },
        }),
      },
    ];
  };

  return (
    <>
      <Modal
        width={1000}
        title={i18n.t('components.SelectTemplateModal.SelectDataFromTemplate')}
        key={'field-parse-module'}
        open={visible}
        onCancel={handleCancel}
        onOk={() => {
          onClearAppend(data?.fieldList);
        }}
      >
        <FormGenerator
          content={getFilterFormContent()}
          form={form}
          useMaxWidth
          onValuesChange={(c, values) => {
            if (Object.keys(c)[0] === 'streamDataTemplate') {
              getData(c['streamDataTemplate']);
            }
          }}
        />
        <div>
          <Table key="templateTable" rowKey="name" columns={columns} dataSource={data?.fieldList} />
        </div>
      </Modal>
    </>
  );
};

export default SelectTemplateModal;
