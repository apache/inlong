// global context
import React from 'react';
import { BreadcrumbProps } from '@/components/Breadcrumb';
export interface GlobalContextProps {
  cluster?: string;
  setCluster?: Function;
  breadMap?: BreadcrumbProps['breadcrumbMap'];
  setBreadMap?: Function;
  userInfo?: any;
}

export default React.createContext<GlobalContextProps>({});
