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

import React, { useState, useCallback, useMemo, useEffect } from 'react';
import {
  Button,
  Card,
  List,
  Col,
  Row,
  Descriptions,
  Input,
  Modal,
  message,
  Space,
  Select,
} from 'antd';
import { RightOutlined } from '@ant-design/icons';
import { PageContainer } from '@/ui/components/PageContainer';
import { useRequest, useSelector } from '@/ui/hooks';
import i18n from '@/i18n';
import request from '@/core/utils/request';
import ClusterList from './ClusterList';
import TagDetailModal from './TagDetailModal';
import styles from './index.module.less';
import { State } from '@/core/stores';
import { timestampFormat } from '@/core/utils';

const Comp: React.FC = () => {
  const [options, setOptions] = useState({
    keyword: '',
    pageSize: 20,
    pageNum: 1,
  });

  const [tagId, setTagId] = useState<number>();
  const [tenantData, setTenantData] = useState([]);

  const [tagDetailModal, setTagDetailModal] = useState<Record<string, unknown>>({
    open: false,
  });

  const roles = useSelector<State, State['roles']>(state => state.roles);

  const {
    data,
    loading,
    run: getList,
  } = useRequest(
    {
      url: '/cluster/tag/listTagByTenantRole',
      method: 'POST',
      data: {
        ...options,
      },
    },
    {
      refreshDeps: [options],
      onSuccess: result => {
        const defaultTag = result?.list?.[0];
        if (defaultTag) {
          setTagId(defaultTag.id);
        }
      },
    },
  );

  const { run: getTenantData } = useRequest(
    {
      url: '/tenant/list',
      method: 'POST',
      data: {
        pageNum: 1,
        pageSize: 9999,
        listByLoginUser: true,
      },
    },
    {
      manual: true,
      onSuccess: result => {
        const list = result?.list?.map(item => ({
          label: item.name,
          value: item.name,
        }));
        setTenantData(list);
      },
    },
  );

  useEffect(() => {
    getTenantData();
  }, []);

  const currentTag = useMemo(() => {
    return data?.list.find(item => item.id === tagId) || {};
  }, [tagId, data]);

  const onListPageChange = (pageNum, pageSize) => {
    setOptions(prev => ({
      ...prev,
      pageNum,
      pageSize,
    }));
  };

  const onEdit = useCallback(({ id }) => {
    setTagDetailModal({ open: true, id });
  }, []);

  const onDelete = useCallback(
    ({ id }) => {
      Modal.confirm({
        title: i18n.t('basic.DeleteConfirm'),
        content: i18n.t('pages.ClusterTags.DelConfirm'),
        onOk: async () => {
          await request({
            url: `/cluster/tag/delete/${id}`,
            method: 'DELETE',
          });
          await getList();
          message.success(i18n.t('basic.DeleteSuccess'));
        },
      });
    },
    [getList],
  );

  return (
    <PageContainer useDefaultBreadcrumb={false} useDefaultContainer={false}>
      <Row gutter={20}>
        <Col style={{ flex: '0 0 430px' }}>
          <Card style={{ height: '100%' }}>
            <List
              size="small"
              itemLayout="horizontal"
              loading={loading}
              pagination={{
                size: 'small',
                onChange: onListPageChange,
                pageSize: 20,
                total: data?.total,
              }}
              dataSource={data?.list}
              header={
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <Space size={[4, 16]} wrap>
                    <Input.Search
                      style={{
                        width:
                          roles?.includes('INLONG_ADMIN') || roles?.includes('INLONG_OPERATOR')
                            ? 150
                            : 180,
                      }}
                      placeholder={i18n.t('pages.ClusterTags.TagPlaceholder')}
                      onSearch={keyword =>
                        setOptions(prev => ({
                          ...prev,
                          keyword,
                        }))
                      }
                    />
                    <Select
                      showSearch
                      allowClear
                      style={{
                        width:
                          roles?.includes('INLONG_ADMIN') || roles?.includes('INLONG_OPERATOR')
                            ? 120
                            : 150,
                      }}
                      placeholder={i18n.t('pages.ClusterTags.TenantPlaceholder')}
                      onChange={keyword =>
                        setOptions(prev => ({
                          ...prev,
                          tenant: keyword,
                        }))
                      }
                      options={tenantData}
                    />
                    <Button
                      type="primary"
                      style={{
                        display:
                          roles?.includes('INLONG_ADMIN') || roles?.includes('INLONG_OPERATOR')
                            ? 'block'
                            : 'none',
                      }}
                      onClick={() => setTagDetailModal({ open: true })}
                    >
                      {i18n.t('basic.Create')}
                    </Button>
                  </Space>
                </div>
              }
              renderItem={(item: Record<string, any>) => (
                <List.Item
                  actions={[
                    {
                      title: i18n.t('basic.Edit'),
                      action: onEdit,
                    },
                    {
                      title: i18n.t('basic.Delete'),
                      action: onDelete,
                    },
                  ].map((k, idx) => (
                    <Button
                      key={idx}
                      type="link"
                      size="small"
                      style={{ padding: 0 }}
                      onClick={e => {
                        e.stopPropagation();
                        k.action(item);
                      }}
                    >
                      {k.title}
                    </Button>
                  ))}
                  className={styles.listItem}
                  onClick={() => setTagId(item.id)}
                >
                  {tagId === item.id && (
                    <RightOutlined style={{ position: 'absolute', left: 0, top: '35%' }} />
                  )}
                  <span className={styles.item}>{item.clusterTag}</span>
                </List.Item>
              )}
            />
          </Card>
        </Col>

        <Col style={{ flex: '1' }}>
          <Card style={{ marginBottom: 20 }}>
            <Descriptions title={currentTag.clusterTag}>
              <Descriptions.Item label={i18n.t('pages.ClusterTags.InCharges')}>
                {currentTag.inCharges}
              </Descriptions.Item>
              <Descriptions.Item label={i18n.t('pages.ClusterTags.Modifier')}>
                {currentTag.modifier}
              </Descriptions.Item>
              <Descriptions.Item label={i18n.t('pages.ClusterTags.ModifyTime')}>
                {timestampFormat(currentTag.modifyTime)}
              </Descriptions.Item>
            </Descriptions>
          </Card>

          <Card>
            <ClusterList clusterTag={currentTag.clusterTag} />
          </Card>
        </Col>
      </Row>

      <TagDetailModal
        {...tagDetailModal}
        open={tagDetailModal.open as boolean}
        onOk={async () => {
          await getList();
          setTagDetailModal({ open: false });
        }}
        onCancel={() => setTagDetailModal({ open: false })}
      />
    </PageContainer>
  );
};

export default Comp;
