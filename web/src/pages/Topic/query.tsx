import * as React from 'react';
import './index.less';
import { OKProps } from '@/components/Modalx';
import { useRequest } from '@/hooks';
import { useContext, useEffect } from 'react';
import GlobalContext from '@/context/globalContext';

interface ComProps {
  fire: string;
  params: any;
  type: string;
}

let newObjectTemp = '';
let editObjectTemp = '';
const Comp = (props: ComProps) => {
  const { fire } = props;
  const { userInfo } = useContext(GlobalContext);
  // eslint-disable-next-line
  useEffect(() => {
    const { params, type } = props;
    dispatchAction(type, params);
  }, [fire]);

  const dispatchAction = (type: string, p: OKProps) => {
    if (!fire) return null;
    let promise;
    switch (type) {
      case 'newTopic':
        promise = newTopic(p);
        break;
      case 'endChooseBroker':
        promise = endChooseBroker(p);
        break;
      case 'editTopic':
        promise = editTopic(p);
        break;
      case 'endEditChooseBroker':
        promise = endEditChooseBroker(p);
        break;
      case 'topicStateChange':
        promise = topicStateChange(type, p);
        break;
      case 'authorizeControl':
        promise = authorizeControl(type, p);
        break;
      case 'deleteTopic':
        promise = deleteTopic(type, p);
        break;
      case 'deleteConsumeGroup':
        promise = deleteConsumeGroup(type, p);
        break;
    }

    promise &&
      promise.then(t => {
        const { callback } = p.params;
        if (t.statusCode !== 0 && callback) callback(t);
      });
  };
  const commonQuery = useRequest<any, any>((url, data) => ({ url, ...data }), {
    manual: true,
  });
  const newTopicQuery = useRequest<any, any>(
    data => ({
      url: '/api/op_query/admin_query_broker_topic_config_info',
      ...data,
    }),
    { manual: true }
  );
  const newTopic = (p: OKProps) => {
    newObjectTemp = JSON.stringify(p.params);
    return newTopicQuery.run({
      data: {
        topicName: '',
        brokerId: '',
      },
    });
  };

  const endChooseBrokerQuery = useRequest<any, any>(
    data => ({ url: '/api/op_modify/admin_add_new_topic_record', ...data }),
    { manual: true }
  );
  const endChooseBroker = (p: OKProps) => {
    const topicParams = JSON.parse(newObjectTemp);
    const { params } = p;
    return endChooseBrokerQuery.run({
      data: {
        borkerId: params.selectBroker.join(','),
        confModAuthToken: p.psw,
        ...topicParams,
      },
    });
  };

  const editTopic = (p: OKProps) => {
    const { params } = p;
    editObjectTemp = JSON.stringify(p.params);
    return newTopicQuery.run({
      data: {
        topicName: params.topicName,
        brokerId: '',
      },
    });
  };
  const endEditChooseBroker = (p: OKProps) => {
    const topicParams = JSON.parse(editObjectTemp);
    const { params } = p;
    return commonQuery.run(`/api/op_modify/admin_modify_topic_info`, {
      data: {
        borkerId: params.selectBroker.join(','),
        confModAuthToken: p.psw,
        ...topicParams,
      },
    });
  };

  const deleteTopic = (type: string, p: OKProps) => {
    const { params } = p;
    return commonQuery.run(`/api/op_modify/admin_delete_topic_info`, {
      data: {
        brokerId: params.selectBroker.join(','),
        confModAuthToken: p.psw,
        modifyUser: userInfo.userName,
        topicName: params.topicName,
      },
    });
  };
  const deleteConsumeGroup = (type: string, p: OKProps) => {
    const { params } = p;
    return commonQuery.run(
      `/api/op_modify/admin_delete_allowed_consumer_group_info`,
      {
        data: {
          groupName: params.groupName,
          confModAuthToken: p.psw,
          topicName: params.topicName,
        },
      }
    );
  };
  const topicStateChange = (type: string, p: OKProps) => {
    const { params } = p;
    const data: any = {
      brokerId: params.selectBroker.join([',']),
      confModAuthToken: p.psw,
      modifyUser: userInfo.userName,
      topicName: params.topicName,
    };
    if (params.type === 'isSrvAcceptPublish') {
      data.acceptPublish = params.value;
    }
    if (params.type === 'isSrvAcceptSubscribe') {
      data.acceptSubscribe = params.value;
    }

    return commonQuery.run(`/api/op_modify/admin_modify_topic_info`, {
      data,
    });
  };

  const authorizeControl = (type: string, p: OKProps) => {
    const { params } = p;
    const data: any = {
      confModAuthToken: p.psw,
      topicName: params.topicName,
      isEnable: params.value,
      modifyUser: userInfo.userName,
    };

    return commonQuery.run(`/api/op_modify/admin_set_topic_authorize_control`, {
      data,
    });
  };

  return <></>;
};

export default Comp;
