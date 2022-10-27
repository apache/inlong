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

import { ColumnType } from 'antd/es/table';
import type { FieldItemType } from '@/metas/common';
import { isDevelopEnv } from '@/utils';
import { DataStatic } from './DataStatic';

interface PositionObjectType extends Record<string, unknown> {
  position: ['before' | 'after', string];
}

function sortListPosition(list: PositionObjectType[], primaryPositionKey = 'name') {
  const output: Record<string, unknown>[] = [];
  const notFoundPosMap: Record<string, PositionObjectType> = {};
  const _list = [...list];
  let loopCount = 0;
  while (_list.length) {
    loopCount++;
    if (loopCount > 500) {
      console.error(
        '[Apache InLong Error] The number of loops of the sorting algorithm array has reached the maximum limit, please check or adjust the configuration.',
      );
      break;
    }
    const listItem = _list.shift();
    if (!listItem) continue;
    if (listItem.position) {
      const [positionType, positionName] = listItem.position;
      const index = output.findIndex(item => item[primaryPositionKey] === positionName);
      if (index !== -1) {
        output.splice(positionType === 'before' ? index : index + 1, 0, listItem);
      } else {
        notFoundPosMap[positionName] = listItem;
      }
    } else {
      output.push(listItem);
    }
    const currentItemName = listItem[primaryPositionKey] as string;
    if (notFoundPosMap[currentItemName]) {
      _list.push(notFoundPosMap[currentItemName]);
      delete notFoundPosMap[currentItemName];
    }
  }

  const notFoundPosList = Object.keys(notFoundPosMap).map(name => notFoundPosMap[name]);
  return output.concat(notFoundPosList);
}

export abstract class DataWithBackend extends DataStatic {
  static FieldList: FieldItemType[] = [];
  static ColumnList: ColumnType<Record<string, any>>[] = [];

  static FormField(config: FieldItemType): PropertyDecorator {
    return (target: any, propertyKey: string) => {
      const { I18nMap, FieldList } = target.constructor;
      const newFieldList = [...FieldList];

      if (isDevelopEnv()) {
        // Hot refresh of the development env will have old data
        const existIndex = newFieldList.findIndex(item => item.name === propertyKey);
        if (existIndex !== -1) {
          newFieldList.splice(existIndex, 1);
        }
      }

      newFieldList.push({
        ...config,
        name: propertyKey,
        label: I18nMap[propertyKey],
      });

      // console.log('--', target.constructor, target.constructor.timer);
      // if (target.constructor.timer) {
      //   clearTimeout(target.constructor.timer);
      // }
      // target.constructor.timer = setTimeout(() => {
      //   sortListPosition(newFieldList);
      // }, 0);
      // const sortedFieldList = sortListPosition(newFieldList);

      target.constructor.FieldList = newFieldList;
    };
  }

  static TableColumn(config?: ColumnType<Record<string, any>>): PropertyDecorator {
    return (target: any, propertyKey: string) => {
      const { I18nMap, ColumnList } = target.constructor;
      const oldIndex = ColumnList.findIndex(item => item.name === propertyKey);
      const subColumnList = [...ColumnList];
      if (oldIndex !== -1) {
        subColumnList.splice(oldIndex, 1);
      }
      subColumnList.push({
        ...(typeof config === 'object' ? config : {}),
        dataIndex: propertyKey,
        title: I18nMap[propertyKey],
      });
      target.constructor.ColumnList = subColumnList;
    };
  }

  abstract parse<T, K>(data: T): K;

  abstract stringify<T, K>(data: T): K;
}
