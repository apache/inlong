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

export const paramReplace = (sinkType, values) => {
  const replaceValues = values;
  if (sinkType === 'ICEBERG' || sinkType === 'DORIS') {
    if (values.backupDatabase === '${database}') {
      replaceValues.databasePattern = values.backupDatabase;
    }
    if (values.backupTable === '${table}') {
      replaceValues.tablePattern = values.backupTable;
    }
  }
  replaceValues.sinkMultipleFormat = 'canal';
  replaceValues.sinkMultipleEnable = true;
  replaceValues.enableCreateResource = 0;
  return replaceValues;
};

export const dataToForm = (sinkType, data) => {
  const sinkData = data;
  if (sinkType === 'ICEBERG' || sinkType === 'DORIS') {
    if (data.databasePattern !== '${database}') {
      sinkData.backupDatabase = 'false';
    }
    if (data.tablePattern !== '${table}') {
      sinkData.backupTable = 'false';
    }
  }
  return sinkData;
};
