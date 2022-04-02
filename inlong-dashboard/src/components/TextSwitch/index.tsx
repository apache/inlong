import React, { useState, useEffect } from 'react';
import i18n from '@/i18n';
import { Button } from 'antd';
import { MinusOutlined, PlusOutlined } from '@/components/Icons';

export interface Props {
  value?: boolean;
  onChange?: (value: boolean) => void;
  title?: string;
}

const TextSwitch: React.FC<Props> = ({
  value = false,
  onChange,
  title = i18n.t('components.TextSwitch.Title'),
}) => {
  const [currentValue, setCurrentValue] = useState(false);

  useEffect(() => {
    if (value !== currentValue) {
      setCurrentValue(value);
    }
    // eslint-disable-next-line
  }, [value]);

  const onToggle = newValue => {
    setCurrentValue(newValue);
    if (onChange && newValue !== value) {
      onChange(newValue);
    }
  };

  return (
    <Button type="link" onClick={() => onToggle(!currentValue)} style={{ padding: 0 }}>
      {value ? <MinusOutlined /> : <PlusOutlined />}
      {title}
    </Button>
  );
};

export default TextSwitch;
