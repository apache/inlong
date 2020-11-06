import { RouteProps } from '@/typings';

const routes: RouteProps[] = [
  {
    path: '/issue/:id',
    component: () => import('@/pages/Issue/consumeGroupDetail'),
  },
  {
    path: '/issue',
    component: () => import('@/pages/Issue'),
  },
  {
    path: '/broker/:id',
    component: () => import('@/pages/Broker/detail'),
  },
  {
    path: '/broker',
    component: () => import('@/pages/Broker'),
  },
  {
    path: '/topic/:name',
    component: () => import('@/pages/Topic/detail'),
  },
  {
    path: '/topic',
    component: () => import('@/pages/Topic'),
  },
  {
    path: '/cluster',
    component: () => import('@/pages/Cluster'),
  },
  {
    component: () => import('@/pages/NotFound'),
  },
];

export default routes;
