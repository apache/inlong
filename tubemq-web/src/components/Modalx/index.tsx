/**
 * TABLE COMPONENT WITH SEARCH
 */
import { Modal, Input } from 'antd';
import * as React from 'react';
import { ModalProps } from 'antd/lib/modal';
import { ReactElement } from 'react';
import './index.less';

const { useState } = React;

export interface OKProps {
  e: React.MouseEvent<HTMLElement>;
  psw: string;
  params?: any;
}

type ComProps = {
  context?: number;
  children?: ReactElement;
  onOk?: (p: OKProps) => {};
  params?: any;
};

const Comp = (props: ComProps & Omit<ModalProps, 'onOk'>) => {
  const { params } = props;
  const [psw, setPsw] = useState('');
  const onOk = (e: React.MouseEvent<HTMLElement>) => {
    props.onOk && props.onOk({ e, psw, params });
  };

  return (
    <>
      <Modal {...props} className="textWrap" width="60%" onOk={onOk}>
        {props.children}
        <div className="psw-set">
          <label className="pws-label">机器授权：</label>
          <Input
            className="psw-input"
            placeholder="请输入机器授权字段，验证操作权限"
            onChange={e => setPsw(e.target.value)}
          />
        </div>
      </Modal>
    </>
  );
};

export default Comp;
