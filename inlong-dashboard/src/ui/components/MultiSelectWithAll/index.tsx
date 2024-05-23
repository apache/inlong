import React, { useState, useEffect } from 'react';
import HighSelect from '@/ui/components/HighSelect';

export const ALL_OPTION_VALUE = 'ALL';
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
