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
import HighSelect from '@/ui/components/HighSelect';

export const ALL_OPTION_VALUE = 'All';
const MultiSelectWithALL = props => {
  const [selectedValues, setSelectedValues] = useState([]);

  const handleSelectChange = (value, option) => {
    let newSelectedValues = [];
    if (value[value.length - 1] === ALL_OPTION_VALUE) {
      newSelectedValues = [ALL_OPTION_VALUE];
    } else {
      newSelectedValues = value.filter(item => item !== ALL_OPTION_VALUE);
    }

    setSelectedValues(newSelectedValues);
    if (props.onChange) {
      props.onChange(newSelectedValues);
    }
  };

  useEffect(() => {
    if ('value' in props) {
      setSelectedValues(props.value || []);
    }
  }, [props.value]);

  return <HighSelect {...props} value={selectedValues} onChange={handleSelectChange} />;
};

export default MultiSelectWithALL;
