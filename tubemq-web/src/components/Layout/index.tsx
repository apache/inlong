import React, { useState, useEffect, useMemo, useContext } from 'react';
import ProBasicLayout, {
  SettingDrawer,
  getMenuData,
  MenuDataItem,
  SettingDrawerProps,
} from '@ant-design/pro-layout';
import { Link } from 'react-router-dom';
import { useLocation } from '@/hooks';
import { isDevelopEnv } from '@/utils';
import initSetting from '@/defaultSettings';
import { menus } from '@/configs';
import './index.less';
import GlobalContext from '@/context/globalContext';

const BasicLayout: React.FC = props => {
  const { cluster, setBreadMap } = useContext(GlobalContext);
  const location = useLocation();
  const [settings, setSetting] = useState<SettingDrawerProps['settings']>(
    initSetting as SettingDrawerProps['settings']
  );
  const [openKeys, setOpenKeys] = useState<string[]>([]);
  const [selectedKeys, setSelectedKeys] = useState<string[]>(['/']);
  const isDev = isDevelopEnv();
  const { pathname } = location;
  const { breadcrumbMap, menuData } = useMemo(() => getMenuData(menus), []);
  // set breadmap 4 children page 2 use
  setBreadMap && setBreadMap(breadcrumbMap);
  useEffect(() => {
    const select = breadcrumbMap.get(pathname);
    if (select) {
      setOpenKeys((select as MenuDataItem)['pro_layout_parentKeys']);
      setSelectedKeys([(select as MenuDataItem)['key'] as string]);
    }
  }, [breadcrumbMap, pathname]);

  return (
    <>
      <ProBasicLayout
        title="TubeMQ"
        logo="/logo192.png"
        menuDataRender={() => menuData}
        menuItemRender={(menuItemProps, defaultDom) => {
          if (menuItemProps.isUrl || !menuItemProps.path) {
            return defaultDom;
          }
          return <Link to={menuItemProps.path}>{defaultDom}</Link>;
        }}
        headerRender={(menuItemProps, defaultDom) => (
          <div className="header-wrapper">
            <span className="header-span">{defaultDom}</span>
            <span>{cluster}</span>
          </div>
        )}
        menuProps={{
          selectedKeys,
          openKeys,
          onOpenChange: setOpenKeys,
        }}
        {...settings}
      >
        {props.children}
      </ProBasicLayout>

      {isDev && (
        <SettingDrawer
          getContainer={() => document.getElementById('root')}
          settings={settings}
          onSettingChange={setSetting}
        />
      )}
    </>
  );
};

export default BasicLayout;
