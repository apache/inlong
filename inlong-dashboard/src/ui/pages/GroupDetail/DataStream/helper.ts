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
  const { inlongStreamId, predefinedField = [], rowTypeFields = [], version, ...rest } = values;

  const fieldList = predefinedField.concat(rowTypeFields).map((item, idx) => ({
    ...item,
    inlongGroupId,
    inlongStreamId,
    isPredefinedField: idx < predefinedField.length ? 1 : 0,
  }));

  const output = {
    ...rest,
    inlongGroupId,
    inlongStreamId,
    version,
  };

  if (fieldList?.length) output.fieldList = fieldList;

  return output;
};

// Convert interface data to form data
export const dataToValues = data => {
  const fieldList = data?.fieldList?.reduce(
    (acc, cur) => {
      cur.isPredefinedField ? acc.predefinedField.push(cur) : acc.rowTypeFields.push(cur);
      return acc;
    },
    {
      predefinedField: [],
      rowTypeFields: [],
    },
  );

  const output = {
    ...data,
    ...fieldList,
  };

  if (data?.predefinedFields !== undefined) {
    const predefinedFields = stringToData(data?.predefinedFields);
    output.predefinedFields = predefinedFields;
  }

  return output;
};

export const dataToString = data => {
  const objs = data.reduce((amount, item) => {
    amount[item.keyName] = item.keyValue;
    return amount;
  }, {});

  const list = arr => {
    let list = [];
    const keys = Object.keys(arr);
    keys.forEach(key => {
      list.push([key, arr[key]].join('='));
    });
    return list;
  };

  return list(objs);
};

export const stringToData = arr => {
  return arr === null || arr === ''
    ? []
    : arr.split('&').reduce((amount, item) => {
        const keyName = item.split('=');
        amount.push({ keyName: keyName[0], keyValue: keyName[1] });
        return amount;
      }, []);
};
