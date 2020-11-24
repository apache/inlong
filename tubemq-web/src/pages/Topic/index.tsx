import React, { useContext } from 'react';
import GlobalContext from '@/context/globalContext';
import Breadcrumb from '@/components/Breadcrumb';
import Table from '@/components/Tablex';
import { Form, Button, Spin, Switch } from 'antd';
import { useImmer } from 'use-immer';
import { useRequest } from '@/hooks';
import tableFilterHelper from '@/components/Tablex/tableFilterHelper';
import { transParamsWithConstantsMap } from '@/utils';
import { TOPIC_INFO_ZH_MAP } from '@/constants/topic';
import './index.less';
import { Link } from 'react-router-dom';
import CommonModal, {
  onOpenModal,
  TopicResultData,
  TopicData,
} from './commonModal';

const Topic: React.FC = () => {
  // column config
  const columns = [
    {
      title: transParamsWithConstantsMap(TOPIC_INFO_ZH_MAP, 'topicName'),
      dataIndex: 'topicName',
      render: (t: Array<any>) => <Link to={'/topic/' + t}>{t}</Link>,
    },
    {
      title: transParamsWithConstantsMap(TOPIC_INFO_ZH_MAP, 'infoCount'),
      dataIndex: 'infoCount',
    },
    {
      title: transParamsWithConstantsMap(TOPIC_INFO_ZH_MAP, 'totalCfgNumPart'),
      dataIndex: 'totalCfgNumPart',
    },
    {
      title: transParamsWithConstantsMap(
        TOPIC_INFO_ZH_MAP,
        'totalRunNumPartCount'
      ),
      dataIndex: 'totalRunNumPartCount',
    },
    {
      title: transParamsWithConstantsMap(
        TOPIC_INFO_ZH_MAP,
        'isSrvAcceptPublish'
      ),
      dataIndex: 'isSrvAcceptPublish',
      render: (t: boolean, r: TopicResultData) => {
        return (
          <Switch
            checked={t}
            onChange={e => onSwitchChange(e, r, 'isSrvAcceptPublish')}
          />
        );
      },
    },
    {
      title: transParamsWithConstantsMap(
        TOPIC_INFO_ZH_MAP,
        'isSrvAcceptSubscribe'
      ),
      dataIndex: 'isSrvAcceptSubscribe',
      render: (t: boolean, r: TopicResultData) => {
        return (
          <Switch
            checked={t}
            onChange={e => onSwitchChange(e, r, 'isSrvAcceptSubscribe')}
          />
        );
      },
    },
    {
      title: transParamsWithConstantsMap(
        TOPIC_INFO_ZH_MAP,
        'enableAuthControl'
      ),
      dataIndex: 'authData.enableAuthControl',
      render: (t: boolean, r: TopicResultData) => {
        return (
          <Switch
            checked={r.authData.enableAuthControl}
            onChange={e => onAuthorizeControl(e, r)}
          />
        );
      },
    },
    {
      title: '操作',
      dataIndex: 'topicIp',
      render: (t: string, r: any) => {
        return <a onClick={() => onDelete(r)}>删除</a>;
      },
    },
  ];
  const { breadMap } = useContext(GlobalContext);
  const [modalParams, updateModelParams] = useImmer<any>({});
  const [filterData, updateFilterData] = useImmer<any>({});
  const [topicList, updateTopicList] = useImmer<TopicData>([]);
  const [form] = Form.useForm();
  // init query
  const { data, loading, run } = useRequest<any, TopicData>(
    (data: TopicResultData) => ({
      url: '/api/op_query/admin_query_topic_info',
      data: data,
    }),
    {
      cacheKey: 'topicList',
      onSuccess: data => {
        updateTopicList(d => {
          Object.assign(d, data);
        });
      },
    }
  );

  // table event
  // acceptSubscribe && acceptPublish options
  const queryBrokerListByTopicNameQuery = useRequest<any, any>(
    data => ({ url: '/api/op_query/admin_query_topic_info', ...data }),
    { manual: true }
  );
  const onSwitchChange = (e: boolean, r: TopicResultData, type: string) => {
    let option = '';
    if (type === 'isSrvAcceptPublish') {
      option = e ? '发布' : '禁止可发布';
    } else if (type === 'isSrvAcceptSubscribe') {
      option = e ? '订阅' : '禁止可订阅';
    }

    queryBrokerListByTopicNameQuery
      .run({
        data: {
          topicName: r.topicName,
          brokerId: '',
        },
      })
      .then((d: TopicResultData) => {
        onOpenModal({
          type: 'topicStateChange',
          title: `请确认操作`,
          updateFunction: updateModelParams,
          params: {
            option,
            value: e,
            topicName: r.topicName,
            data: d[0].topicInfo,
            type,
            callback: () => {
              const index = data.findIndex(
                (t: TopicResultData) => t.topicName === r.topicName
              );
              updateTopicList(d => {
                d[index][type] = e + '';
              });
            },
          },
        });
      });
  };
  // author
  const onAuthorizeControl = (e: boolean, r: TopicResultData) => {
    const option = e ? '发布' : '禁止可发布';
    onOpenModal({
      type: 'authorizeControl',
      title: `请确认操作`,
      updateFunction: updateModelParams,
      params: {
        option,
        value: e,
        topicName: r.topicName,
        callback: () => {
          const index = data.findIndex(
            (t: TopicResultData) => t.topicName === r.topicName
          );
          updateTopicList(d => {
            d[index]['authData']['enableAuthControl'] = e + '';
          });
        },
      },
    });
  };
  // new topic
  const onNewTopic = () => {
    onOpenModal({
      type: 'newTopic',
      title: '新建Topic',
      updateFunction: updateModelParams,
      params: {
        callback: (d: any) => {
          onOpenModal({
            type: 'chooseBroker',
            title: '选择【新增】broker列表',
            updateFunction: updateModelParams,
            params: {
              data: d,
              callback: () => {
                onOpenModal({
                  type: 'close',
                  updateFunction: updateModelParams,
                });
              },
            },
          });
        },
      },
    });
  };
  // delete
  const onDelete = (r: TopicResultData) => {
    queryBrokerListByTopicNameQuery
      .run({
        data: {
          topicName: r.topicName,
          brokerId: '',
        },
      })
      .then((d: TopicResultData) => {
        onOpenModal({
          type: 'deleteTopic',
          title: `请确认操作`,
          updateFunction: updateModelParams,
          params: {
            topicName: r.topicName,
            data: d[0].topicInfo,
          },
        });
      });
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
            <Form.Item>
              <Button
                type="primary"
                onClick={() => onNewTopic()}
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
          columns={columns}
          dataSource={topicList}
          rowKey="topicName"
          searchPlaceholder="请输入topicName查询"
          searchWidth={12}
          dataSourceX={filterData.list}
          filterFnX={value =>
            tableFilterHelper({
              key: value,
              srcArray: data,
              targetArray: filterData.list,
              updateFunction: res =>
                updateFilterData(filterData => {
                  filterData.list = res;
                }),
              filterList: ['topicName'],
            })
          }
        />
      </div>
      <CommonModal modalParams={modalParams} data={data} />
    </Spin>
  );
};

export default Topic;
