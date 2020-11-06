import React, { useContext } from 'react';
import GlobalContext from '@/context/globalContext';
import Breadcrumb from '@/components/Breadcrumb';
import Table from '@/components/Tablex';
import tableFilterHelper from '@/components/Tablex/tableFilterHelper';
import { Spin } from 'antd';
import { useImmer } from 'use-immer';
import './index.less';
import { useRequest } from '@/hooks';
import { useParams } from 'react-router-dom';

declare type ConsumeGroupData = any[];
interface ConsumeGroupQueryData {
  consumeGroup: string;
}

// column config
const columns = [
  {
    title: '消费者ID',
    dataIndex: 'consumerId',
  },
  {
    title: '消费Topic',
    dataIndex: 'topicName',
  },
  {
    title: 'broker地址',
    dataIndex: 'brokerAddr',
  },
  {
    title: '分区ID',
    dataIndex: 'partId',
  },
];

const queryUser = (data: ConsumeGroupQueryData) => ({
  url: '/api/op_query/admin_query_consume_group_detail',
  data: data,
});

const ConsumeGroupDetail: React.FC = () => {
  const { id } = useParams();
  const { breadMap } = useContext(GlobalContext);
  const [filterData, updateFilterData] = useImmer<any>({});
  const { data, loading } = useRequest<any, ConsumeGroupData>(
    () =>
      queryUser({
        consumeGroup: id,
      }),
    {
      formatResult: data => {
        const d = data[0];
        return {
          list: d.parInfo.map((t: any) => ({
            consumerId: d.consumerId,
            ...t,
          })),
        };
      },
    }
  );

  return (
    <Spin spinning={loading}>
      <Breadcrumb
        breadcrumbMap={breadMap}
        appendParams={`消费组详情（${id}）`}
      ></Breadcrumb>
      <div className="main-container">
        <Table
          columns={columns}
          dataSource={data?.list}
          rowKey="brokerAddr"
          searchPlaceholder="请输入 broker地址/分区ID 搜索"
          dataSourceX={filterData.list}
          filterFnX={value =>
            tableFilterHelper({
              key: value,
              srcArray: data?.list,
              targetArray: filterData.list,
              updateFunction: res =>
                updateFilterData(filterData => {
                  filterData.list = res;
                }),
              filterList: ['brokerAddr', 'partId'],
            })
          }
        ></Table>
      </div>
    </Spin>
  );
};

export default ConsumeGroupDetail;
