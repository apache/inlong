import React, { useContext, useState } from 'react';
import GlobalContext from '@/context/globalContext';
import Breadcrumb from '@/components/Breadcrumb';
import Table from '@/components/Tablex';
import { Form, Select, Button, Spin, Switch, message } from 'antd';
import { useImmer } from 'use-immer';
import { useRequest } from '@/hooks';
import tableFilterHelper from '@/components/Tablex/tableFilterHelper';
import { boolean2Chinese, transParamsWithConstantsMap } from '@/utils';
import { BROKER_INFO_ZH_MAP } from '@/constants/broker';
import './index.less';
import { Link } from 'react-router-dom';
import CommonModal, {
  OPTIONS,
  onOpenModal,
  BrokerResultData,
  BrokerData,
} from './commonModal';

const { Option } = Select;
const Broker: React.FC = () => {
  // column config
  const columns = [
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'brokerId'),
      dataIndex: 'brokerId',
      fixed: 'left',
      render: (t: Array<any>) => <Link to={'/broker/' + t}>{t}</Link>,
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'brokerIp'),
      dataIndex: 'brokerIp',
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'brokerPort'),
      dataIndex: 'brokerPort',
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'manageStatus'),
      dataIndex: 'manageStatus',
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'runStatus'),
      dataIndex: 'runStatus',
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'subStatus'),
      dataIndex: 'subStatus',
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'acceptPublish'),
      dataIndex: 'acceptPublish',
      render: (t: string, r: BrokerResultData) => {
        return (
          <Switch
            checked={t === 'true'}
            onChange={e => onSwitchChange(e, r, 'acceptPublish')}
          />
        );
      },
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'acceptSubscribe'),
      dataIndex: 'acceptSubscribe',
      render: (t: string, r: BrokerResultData) => {
        return (
          <Switch
            checked={t === 'true'}
            onChange={e => onSwitchChange(e, r, 'acceptSubscribe')}
          />
        );
      },
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'isConfChanged'),
      dataIndex: 'isConfChanged',
      render: (t: string) => boolean2Chinese(t),
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'isConfLoaded'),
      dataIndex: 'isConfLoaded',
      render: (t: string) => boolean2Chinese(t),
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'isBrokerOnline'),
      dataIndex: 'isBrokerOnline',
      render: (t: string) => boolean2Chinese(t),
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'acceptPublish'),
      dataIndex: 'isBrokerOnline',
      render: (t: string) => boolean2Chinese(t),
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'brokerTLSPort'),
      dataIndex: 'brokerTLSPort',
      render: (t: string) => boolean2Chinese(t),
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'enableTLS'),
      dataIndex: 'enableTLS',
      render: (t: boolean) => boolean2Chinese(t),
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'isRepAbnormal'),
      dataIndex: 'isRepAbnormal',
      render: (t: boolean) => boolean2Chinese(t),
    },
    {
      title: transParamsWithConstantsMap(BROKER_INFO_ZH_MAP, 'isAutoForbidden'),
      dataIndex: 'isAutoForbidden',
      render: (t: boolean) => boolean2Chinese(t),
    },
    {
      title: '操作',
      dataIndex: 'brokerIp',
      fixed: 'right',
      width: 180,
      render: (t: string, r: any) => {
        return (
          <span className="options-wrapper">
            {OPTIONS.map(t => (
              <a key={t.value} onClick={() => onOptionsChange(t.value, r)}>
                {t.name}
              </a>
            ))}
          </span>
        );
      },
    },
  ];
  const { breadMap } = useContext(GlobalContext);
  const [modalParams, updateModelParams] = useImmer<any>({});
  const [filterData, updateFilterData] = useImmer<any>({});
  const [selectBroker, setSelectBroker] = useState<any>([]);
  const [brokerList, updateBrokerList] = useImmer<BrokerData>([]);
  const [form] = Form.useForm();
  // init query
  const { data, loading, run } = useRequest<any, BrokerData>(
    (data: BrokerResultData) => ({
      url: '/api/op_query/admin_query_broker_run_status',
      data: data,
    }),
    {
      onSuccess: data => {
        updateBrokerList(d => {
          Object.assign(d, data);
        });
      },
    }
  );

  // table event
  // acceptSubscribe && acceptPublish options
  const onSwitchChange = (e: boolean, r: BrokerResultData, type: string) => {
    let option = '';
    if (type === 'acceptPublish') {
      option = e ? '发布' : '禁止可发布';
    } else if (type === 'acceptSubscribe') {
      option = e ? '订阅' : '禁止可订阅';
    }

    onOpenModal({
      type: 'brokerStateChange',
      title: `请确认操作`,
      updateFunction: updateModelParams,
      params: {
        option,
        id: r.brokerId,
        type,
        callback: () => {
          const index = data.findIndex(
            (t: BrokerResultData) => t.brokerId === r.brokerId
          );
          updateBrokerList(d => {
            d[index][type] = e + '';
          });
        },
      },
    });
  };
  // new broker
  const onNewBroker = () => {
    onOpenModal({
      type: 'newBroker',
      title: '新建Broker',
      updateFunction: updateModelParams,
    });
  };
  // online, offline, etc.
  const onOptionsChange = (type: string, r?: BrokerResultData) => {
    if (!r && !selectBroker.length) {
      form.resetFields();
      return message.error('批量操作至少选择一列！');
    }

    onOpenModal({
      type,
      title: `确认进行【${OPTIONS.find(t => t.value === type)?.name}】操作？`,
      updateFunction: updateModelParams,
      params: r ? [r.brokerId] : selectBroker,
    });
  };
  // table select
  const onBrokerTableSelectChange = (p: any[]) => {
    setSelectBroker(p);
  };

  return (
    <Spin spinning={loading}>
      <Breadcrumb breadcrumbMap={breadMap} />
      <div className="main-container">
        <div
          className="search-wrapper"
          style={{ float: 'right', marginRight: '-16px' }}
        >
          <Form form={form} layout={'inline'}>
            <Form.Item label="批量操作" name="optionType">
              <Select
                style={{ width: 120 }}
                onChange={(v: string) => onOptionsChange(v)}
                placeholder="请选择操作"
              >
                {OPTIONS.map(t => (
                  <Option value={t.value} key={t.value}>
                    {t.name}
                  </Option>
                ))}
              </Select>
            </Form.Item>
            <Form.Item>
              <Button
                type="primary"
                onClick={() => onNewBroker()}
                style={{ margin: '0 10px 0 10px' }}
              >
                新增
              </Button>
              <Button type="primary" onClick={() => run()}>
                刷新
              </Button>
            </Form.Item>
          </Form>
        </div>
        <Table
          rowSelection={{ onChange: onBrokerTableSelectChange }}
          columns={columns}
          dataSource={brokerList}
          rowKey="brokerId"
          searchPlaceholder="请输入关键字搜索"
          searchWidth={12}
          dataSourceX={filterData.list}
          scroll={{ x: 2500 }}
          filterFnX={value =>
            tableFilterHelper({
              key: value,
              srcArray: data,
              targetArray: filterData.list,
              updateFunction: res =>
                updateFilterData(filterData => {
                  filterData.list = res;
                }),
              filterList: [
                'brokerId',
                'brokerIp',
                'brokerPort',
                'runStatus',
                'subStatus',
                'manageStatus',
              ],
            })
          }
        />
      </div>
      <CommonModal modalParams={modalParams} data={data} />
    </Spin>
  );
};

export default Broker;
