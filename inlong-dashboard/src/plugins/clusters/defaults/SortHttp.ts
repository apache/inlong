import { ClusterInfo } from '@/plugins/clusters/common/ClusterInfo';
import { DataWithBackend } from '@/plugins/DataWithBackend';
import { RenderRow } from '@/plugins/RenderRow';
import { RenderList } from '@/plugins/RenderList';

export default class SortHttp
  extends ClusterInfo
  implements DataWithBackend, RenderRow, RenderList {}
