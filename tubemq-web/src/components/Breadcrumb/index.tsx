import React from 'react';
import { MenuDataItem } from '@ant-design/pro-layout';
import { Link } from 'react-router-dom';
import { useLocation } from '@/hooks';
import './index.less';
import { Breadcrumb } from 'antd';

export interface BreadcrumbProps {
  breadcrumbMap?: Map<string, import('@umijs/route-utils').MenuDataItem>;
  appendParams?: string;
}

const BasicLayout: React.FC<BreadcrumbProps> = props => {
  const location = useLocation();
  const { breadcrumbMap, appendParams } = props;

  const pathSnippets = location.pathname.split('/').filter(i => i);
  const breadcrumbItems = pathSnippets.map((_, index) => {
    const breadcrumbNameMap = {} as any;
    breadcrumbMap &&
      breadcrumbMap.forEach((t: MenuDataItem) => {
        breadcrumbNameMap[t.key as string] = t.name;
      });
    const url = `/${pathSnippets.slice(0, index + 1).join('/')}`;
    if (appendParams && index === pathSnippets.length - 1) {
      return (
        <Breadcrumb.Item key={url}>
          <Link to={url}>{appendParams}</Link>
        </Breadcrumb.Item>
      );
    }

    return (
      <Breadcrumb.Item key={url}>
        <Link to={url}>{breadcrumbNameMap[url]}</Link>
      </Breadcrumb.Item>
    );
  });

  return (
    <>
      <Breadcrumb className="breadcrumb-wrapper">{breadcrumbItems}</Breadcrumb>
    </>
  );
};

export default BasicLayout;
