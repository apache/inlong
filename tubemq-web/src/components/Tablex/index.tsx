/**
 * TABLE COMPONENT WITH SEARCH
 */
import { Table, Input, Row, Button, Col, Tooltip } from 'antd';
import * as React from 'react';
import { TableProps } from 'antd/lib/table';
import { CaretDownFilled, CaretUpFilled } from '@ant-design/icons';
import { isEmptyParam } from '@/utils';
import { useEffect } from 'react';
import './index.less';

const { useState } = React;

const { Search } = Input;

interface ComProps extends TableProps<any> {
  filterFnX?: (value: any) => void;
  columns?: any;
  dataSourceX?: any;
  searchPlaceholder?: string;
  defaultSearchKey?: string;
  isTruePagination?: boolean;
  showSearch?: boolean;
  searchWidth?: number;
  searchStyle?: any;
}

const Comp = (props: ComProps) => {
  const {
    columns,
    filterFnX,
    searchPlaceholder,
    expandable,
    defaultSearchKey,
    isTruePagination,
    showSearch = true,
    searchWidth = 8,
    searchStyle = {},
  } = props;
  const [filterKey, setFilterKey] = useState(defaultSearchKey);
  // 自动增加排序
  if (columns) {
    columns.forEach((t: any) => {
      t.sorter = (a: any, b: any) =>
        a[(t as any).dataIndex] - b[(t as any).dataIndex] >= 0 ? 1 : -1;
    });
  }
  if (expandable && !expandable.expandIcon) {
    expandable.expandIcon = ({ expanded, onExpand, record }) =>
      expanded ? (
        <CaretUpFilled onClick={e => onExpand(record, e)} />
      ) : (
        <CaretDownFilled onClick={e => onExpand(record, e)} />
      );
  }
  const opts = { ...props };
  const dataSource =
    (!filterKey && isEmptyParam(opts.dataSourceX)) || isTruePagination
      ? opts.dataSource
      : opts.dataSourceX;
  // 如有默认，先处理一次
  useEffect(() => {
    if (defaultSearchKey === undefined) return;

    setFilterKey(defaultSearchKey || '');
    filterFnX && filterFnX(defaultSearchKey || '');
  }, [defaultSearchKey, filterFnX]);
  // 分页如果只有一页，自动隐藏
  opts.pagination = Object.assign(
    {
      hideOnSinglePage: true,
    },
    {
      ...opts.pagination,
    }
  );

  const onChange = (e: any) => {
    if (!isTruePagination) {
      filterFnX && filterFnX(e.target.value);
    }

    setFilterKey(e.target.value);
  };

  return (
    <>
      {showSearch && filterFnX && (
        <Row gutter={20} className="mb10" style={{ position: 'relative' }}>
          <Col span={searchWidth} style={{ padding: 0, ...searchStyle }}>
            <Tooltip title={filterKey}>
              <Search
                value={filterKey}
                onChange={onChange}
                onSearch={v => filterFnX(v)}
                allowClear
                placeholder={searchPlaceholder || '字符串大小写敏感'}
                enterButton={
                  <Button type="primary" onClick={filterFnX}>
                    搜索
                  </Button>
                }
              />
            </Tooltip>
          </Col>
        </Row>
      )}

      <Table {...opts} dataSource={dataSource} className="textWrap" />
    </>
  );
};

export default Comp;
