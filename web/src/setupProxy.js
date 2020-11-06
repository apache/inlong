// eslint-disable-next-line @typescript-eslint/no-var-requires
const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function(app) {
  app.use(
    createProxyMiddleware('/webapi.htm', {
      target: 'http://127.0.0.1:8080', // should set api address
      changeOrigin: true,
      ws: true,
    })
  );
};
