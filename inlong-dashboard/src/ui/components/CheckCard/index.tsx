/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useState, useEffect } from 'react';
import { Card, Col, Row, theme, Tooltip } from 'antd';
import { DoubleRightOutlined, DatabaseOutlined } from '@ant-design/icons';
import styles from './index.module.less';

export interface CheckCardOption {
  label: string;
  value: string | number;
  image?: string | Promise<{ default: string }>;
  isSmallImage?: boolean;
}

export interface CheckCardProps {
  value?: string | number;
  onChange?: (value: boolean) => void;
  options?: CheckCardOption[];
  disabled: boolean;
  span?: number;
}

const { useToken } = theme;

const CheckCard: React.FC<CheckCardProps> = ({ options, value, onChange, disabled, span = 6 }) => {
  const [currentValue, setCurrentValue] = useState(value);

  const [logoMap, setLogoMap] = useState({});

  const [isExpand, setExpandStatus] = useState(!Boolean(currentValue));

  const [smallImageMap, setSmallImageMap] = useState({});
  const { token } = useToken();

  useEffect(() => {
    if (value !== currentValue) {
      setCurrentValue(value);
      setExpandStatus(false);
    }
    // eslint-disable-next-line
  }, [value]);

  useEffect(() => {
    /*
      The dynamic loading of logo images is based on the "label" property of the option.
      Since Vite does not support require.context, import() is used instead.
      It also can be used to determine whether the image exists and handle the module cache.
    */
    (async () => {
      setLogoMap(
        (await Promise.allSettled(options.map(option => option.image))).reduce(
          (res, item, index) => {
            if (item.status === 'fulfilled' && item.value) {
              const url = typeof item.value === 'string' ? item.value : item.value.default;
              res[options[index].label] = url;
            }
            return res;
          },
          {},
        ),
      );
    })();
  }, [options]);

  useEffect(() => {
    setSmallImageMap(
      options
        .filter(item => item.isSmallImage)
        .reduce((acc, item) => {
          acc[item.label] = item.isSmallImage;
          return acc;
        }, {}),
    );
  }, [options]);
  const handleCardClick = newValue => {
    setExpandStatus(false);
    if (newValue !== currentValue) {
      setCurrentValue(newValue);
      if (onChange) {
        onChange(newValue);
      }
    }
  };

  const renderContent = label => (
    <Tooltip placement="top" title={label}>
      <div className={styles.cardInfo}>
        {logoMap[label] ? (
          !smallImageMap[label] ? (
            <img height="100%" alt={label} src={logoMap[label]}></img>
          ) : (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                width: '100%',
              }}
            >
              <img style={{ marginLeft: 10 }} height="100%" alt={label} src={logoMap[label]}></img>
              <span style={{ marginLeft: 10 }}>{label}</span>
            </div>
          )
        ) : (
          <>
            <DatabaseOutlined style={{ fontSize: 20 }} />
            <span>{label}</span>
          </>
        )}
      </div>
    </Tooltip>
  );
  console.log(smallImageMap);
  return (
    <Row gutter={15} className={styles.cardRow}>
      {!isExpand ? (
        <>
          <Col span={span} className={styles.cardCol}>
            <Card
              size="small"
              bodyStyle={{ textAlign: 'center' }}
              className={disabled ? styles.cardDisabled : ''}
            >
              {renderContent(options.find(item => item.value === currentValue)?.label)}
            </Card>
          </Col>
          {!disabled && (
            <Col style={{ display: 'flex', alignItems: 'center' }}>
              <DoubleRightOutlined
                className={styles.editIcon}
                onClick={() => setExpandStatus(true)}
              />
            </Col>
          )}
        </>
      ) : (
        options.map(item => (
          <Col span={span} key={item.value} className={styles.cardCol} title={item.label}>
            <Card
              hoverable
              size="small"
              onClick={() => handleCardClick(item.value)}
              style={item.value === currentValue ? { borderColor: token.colorPrimary } : {}}
            >
              {renderContent(item.label)}
            </Card>
          </Col>
        ))
      )}
    </Row>
  );
};

export default CheckCard;
