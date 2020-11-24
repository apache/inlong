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

const Comp = (props: ComProps) => {
  const { fire } = props;
  const { userInfo } = useContext(GlobalContext);
  // eslint-disable-next-line
  useEffect(() => {
    const { params, type } = props;
    dispatchAction(type, params);
  }, [fire, props]);

  const dispatchAction = (type: string, p: OKProps) => {
    if (!fire) return null;
    let promise;
    switch (type) {
      case 'newBroker':
        promise = newBroker(p);
        break;
      case 'editBroker':
        promise = editBroker(p);
        break;
      case 'brokerStateChange':
        promise = brokerAcceptPublish(type, p);
        break;
      case 'online':
      case 'offline':
      case 'reload':
      case 'delete':
        promise = brokerOptions(type, p);
        break;
    }

    promise &&
      promise.then(t => {
        const { callback } = p.params;
        if (t.statusCode !== 0 && callback) callback(t);
      });
  };

  const newBrokerQuery = useRequest<any, any>(
    data => ({ url: '/api/op_modify/admin_add_broker_configure', ...data }),
    { manual: true }
  );
  const newBroker = (p: OKProps) => {
    const { params } = p;
    return newBrokerQuery.run({
      data: {
        ...params,
        confModAuthToken: p.psw,
        createUser: userInfo.userName,
      },
    });
  };

  const updateBrokerQuery = useRequest<any, any>(
    data => ({ url: '/api/op_modify/admin_update_broker_configure', ...data }),
    { manual: true }
  );
  const editBroker = (p: OKProps) => {
    const { params } = p;
    return updateBrokerQuery.run({
      data: {
        ...params,
        confModAuthToken: p.psw,
        createUser: userInfo.userName,
      },
    });
  };

  const brokerOptionsQuery = useRequest<any, any>(
    (url, data) => ({ url, ...data }),
    { manual: true }
  );
  const brokerOptions = (type: string, p: OKProps) => {
    const { params } = p;
    return brokerOptionsQuery.run(
      `/api/op_modify/admin_${type}_broker_configure`,
      {
        data: {
          brokerId: params ? params?.join(',') : params?.selectBroker.join(','),
          confModAuthToken: p.psw,
          createUser: userInfo.userName,
        },
      }
    );
  };

  const brokerAcceptPublishQuery = useRequest<any, any>(
    (url, data) => ({ url, ...data }),
    { manual: true }
  );
  const brokerAcceptPublish = (type: string, p: OKProps) => {
    const { params } = p;
    const data: any = {
      brokerId: params.id,
      confModAuthToken: p.psw,
      createUser: userInfo.userName,
    };
    if (params.type === 'acceptPublish') {
      data.isAcceptPublish = params.option;
    }
    if (params.type === 'acceptSubscribe') {
      data.isAcceptSubscribe = params.option;
    }

    return brokerAcceptPublishQuery.run(
      `/api/op_modify/admin_set_broker_read_or_write`,
      {
        data,
      }
    );
  };

  return <></>;
};

export default Comp;
