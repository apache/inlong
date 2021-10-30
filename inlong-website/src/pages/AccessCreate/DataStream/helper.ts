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

// Convert form data into interface submission data format
export const valuesToData = (values, inlongGroupId) => {
  const array = values.map(item => {
    const {
      inlongStreamId,
      predefinedFields = [],
      rowTypeFields = [],
      dataSourceType,
      dataSourceBasicId,
      dataSourcesConfig = [],
      dataStorage = [],
      ...rest
    } = item;
    const output = {} as any;
    if (dataSourceType === 'DB' || dataSourceType === 'FILE') {
      const dstLow = dataSourceType.toLowerCase();
      output[`${dstLow}BasicInfo`] = {
        inlongGroupId,
        inlongStreamId,
      };
      if (dataSourceBasicId !== undefined) {
        output[`${dstLow}BasicInfo`].id = dataSourceBasicId;
      }
      output[`${dstLow}DetailInfoList`] = dataSourcesConfig.map(k => ({
        ...k,
        inlongGroupId,
        inlongStreamId,
      }));
    }

    output.storageInfo = dataStorage.reduce((acc, type) => {
      if (!type) return acc;

      const data = rest[`dataStorage${type}`] || [];
      delete rest[`dataStorage${type}`];
      const formatData = data.map(ds => ({
        ...ds,
        inlongGroupId,
        inlongStreamId,
        storageType: type,
      }));

      return acc.concat(formatData);
    }, []);

    const fieldList = predefinedFields.concat(rowTypeFields).map((item, idx) => ({
      ...item,
      inlongGroupId,
      inlongStreamId,
      isPredefinedField: idx < predefinedFields.length ? 1 : 0,
    }));

    output.streamInfo = {
      ...rest,
      inlongGroupId,
      inlongStreamId,
      inCharges: rest.inCharges?.join(','),
      dataSourceType,
    };

    if (fieldList?.length) output.streamInfo.fieldList = fieldList;

    return output;
  });

  return array;
};

// Convert interface data to form data
export const dataToValues = data => {
  const array = data.map(item => {
    const {
      fileBasicInfo,
      fileDetailInfoList,
      dbBasicInfo,
      dbDetailInfoList,
      storageInfo,
      streamInfo,
    } = item;
    let output = {} as any;
    const dataSourceType = fileBasicInfo ? 'FILE' : dbBasicInfo ? 'DB' : '';
    output.dataSourceType = dataSourceType;
    if (dataSourceType === 'DB') {
      output = {
        ...output,
        ...dbBasicInfo,
      };
      output.dataSourceBasicId = dbBasicInfo.id;
      output.dataSourcesConfig = dbDetailInfoList;
    } else if (dataSourceType === 'FILE') {
      output = {
        ...output,
        ...fileBasicInfo,
      };
      output.dataSourceBasicId = fileBasicInfo.id;
      output.dataSourcesConfig = fileDetailInfoList;
    }

    storageInfo.forEach(({ storageType, ...item }) => {
      if (!output[`dataStorage${storageType}`]) output[`dataStorage${storageType}`] = [];
      output[`dataStorage${storageType}`].push(item);
    });
    output.dataStorage = storageInfo.map(item => item.storageType);

    const fieldList = streamInfo.fieldList?.reduce(
      (acc, cur) => {
        cur.isPredefinedField ? acc.predefinedFields.push(cur) : acc.rowTypeFields.push(cur);
        return acc;
      },
      {
        predefinedFields: [],
        rowTypeFields: [],
      },
    );

    output = {
      ...output,
      ...fieldList,
      ...streamInfo,
      inCharges: streamInfo.inCharges?.split(','),
    };

    return output;
  });

  return array;
};
