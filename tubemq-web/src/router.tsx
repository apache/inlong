import React, { Suspense, lazy, useState } from 'react';
import {
  BrowserRouter as Router,
  Switch,
  Route,
  Redirect,
} from 'react-router-dom';
import { PageLoading } from '@ant-design/pro-layout';
import { hot } from 'react-hot-loader/root';
import { Layout } from '@/components';
import routes from '@/routes';
import GlobalContext from '@/context/globalContext';

const App = () => {
  const [cluster, setCluster] = useState();
  const [breadMap, setBreadMap] = useState();
  // eslint-disable-next-line
  const [userInfo, setUserInfo] = useState({
    userName: 'webapi',
  });

  return (
    <GlobalContext.Provider
      value={{ cluster, setCluster, breadMap, setBreadMap, userInfo }}
    >
      <Router>
        <Layout>
          <Suspense fallback={<PageLoading />}>
            <Switch>
              {routes.map((route, index: number) => (
                <Route
                  key={index}
                  path={route.path}
                  exact={route.exact}
                  strict={route.strict}
                  render={props => {
                    const LazyComponent = lazy(route.component);
                    return <LazyComponent {...props} />;
                  }}
                />
              ))}
            </Switch>
            <Route
              exact
              path="/"
              render={() => <Redirect to="/issue" push />}
            />
          </Suspense>
        </Layout>
      </Router>
    </GlobalContext.Provider>
  );
};

export default process.env.NODE_ENV === 'development' ? hot(App) : App;
