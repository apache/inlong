import React, { useContext } from 'react';
import GlobalContext from '@/context/globalContext';
import Breadcrumb from '@/components/Breadcrumb';
import Table from '@/components/Tablex';
import { Form, Input, Button, Spin } from 'antd';
import { useImmer } from 'use-immer';
import './index.less';
import { useRequest } from '@/hooks';
import { Link } from 'react-router-dom';

declare type IssueData = any[];
interface IssueQueryData {
  topicName?: string;
  consumeGroup?: string;
}

// column config
const columns = [
  {
    title: '消费组',
    dataIndex: 'consumeGroup',
    render: (t: Array<any>) => <Link to={'/issue/' + t}>{t}</Link>,
  },
  {
    title: '消费Topic',
    dataIndex: 'topicSet',
    render: (t: Array<any>) => {
      return t.join(',');
    },
  },
  {
    title: '消费分区',
    dataIndex: 'consumerNum',
  },
];

const queryIssueList = (data: IssueQueryData) => ({
  url: '/api/op_query/admin_query_sub_info',
  data: data,
});

const Issue: React.FC = () => {
  const { breadMap } = useContext(GlobalContext);
  const [form] = Form.useForm();
  const [formValues, updateFormValues] = useImmer<any>({});
  const { data, loading, run } = useRequest<any, IssueData>(queryIssueList, {});

  const onValuesChange = (p: any) => {
    updateFormValues(d => {
      Object.assign(d, p);
    });
  };
  const onSearch = () => {
    run(formValues);
  };

  const onReset = () => {
    form.resetFields();
    run({});
  };

  return (
    <Spin spinning={loading}>
      <Breadcrumb breadcrumbMap={breadMap}></Breadcrumb>
      <div className="main-container">
        <div className="search-wrapper">
          <Form form={form} layout={'inline'} onValuesChange={onValuesChange}>
            <Form.Item label="Topic 名称" name="topicName">
              <Input placeholder="" />
            </Form.Item>
            <Form.Item label="消费组" name="consumeGroup">
              <Input placeholder="" />
            </Form.Item>
            <Form.Item>
              <Button
                type="primary"
                onClick={onSearch}
                style={{ margin: '0 20px' }}
              >
                查询
              </Button>
              <Button type="default" onClick={onReset}>
                重置
              </Button>
            </Form.Item>
          </Form>
        </div>
        <Table
          columns={columns}
          dataSource={data}
          rowKey="consumeGroup"
        ></Table>
      </div>
    </Spin>
  );
};

export default Issue;
