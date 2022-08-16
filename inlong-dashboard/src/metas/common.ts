import type { FormItemProps } from '@/components/FormGenerator';
import { ColumnType } from 'antd/es/table';
import { excludeObject } from '@/utils';

export interface FieldItemType extends FormItemProps {
  position?: string[];
  _renderTable?: boolean | ColumnType<Record<string, any>>;
}

export const genFields = (
  fieldsDefault: FieldItemType[],
  fieldsExtends: FieldItemType[],
): FormItemProps[] => {
  const output: FieldItemType[] = [];
  const fields = fieldsDefault.concat(fieldsExtends);
  while (fields.length) {
    const fieldItem = fields.shift();
    if (fieldItem.position) {
      const [positionType, positionName] = fieldItem.position;
      const index = output.findIndex(item => item.name === positionName);
      if (index !== -1) {
        output.splice(positionType === 'before' ? index : index + 1, 0, fieldItem);
      } else {
        fields.push(fieldItem);
      }
    } else {
      output.push(fieldItem);
    }
  }

  return output;
};

export const genForm = (fields: Omit<FieldItemType, 'position'>[]): FormItemProps[] => {
  return fields.map(item => excludeObject(['_renderTable'], item));
};

export const genTable = (
  fields: Omit<FieldItemType, 'position'>[],
): ColumnType<Record<string, any>>[] => {
  return fields
    .filter(item => Boolean(item._renderTable))
    .map(item => {
      let output: ColumnType<Record<string, any>> = {
        title: item.label,
        dataIndex: item.name,
      };
      if (typeof item._renderTable === 'object') {
        output = {
          ...output,
          ...item._renderTable,
        };
      }
      return output;
    });
};
