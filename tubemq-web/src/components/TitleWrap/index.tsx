import * as React from 'react';
import './index.less';

interface ComProps {
  title: any;
  children?: any;
  wrapperStyle?: any;
  hasSplit?: boolean;
}

const Comp = (props: ComProps) => {
  const { hasSplit = true } = props;

  return (
    <div style={props.wrapperStyle} className={hasSplit ? 'split-border' : ''}>
      <div className="title-wrap-title">{props.title}</div>
      {props.children}
    </div>
  );
};

export default Comp;
