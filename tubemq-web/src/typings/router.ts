import { Route as LayoutRoute } from '@ant-design/pro-layout/lib/typings';
import { RouteProps as ReactRouteProps } from 'react-router-dom';

export interface Route extends LayoutRoute {
  paths?: string[];
}

export type OmitRouteProps = Omit<ReactRouteProps, 'component'> & {
  component: () => Promise<{ default: any }>;
};

export interface RouteProps extends OmitRouteProps {
  component: () => Promise<{ default: any }>;
}
