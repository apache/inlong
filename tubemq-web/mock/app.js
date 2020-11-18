const { apiPrefix } = require('./_constant');
const packageJSON = require('../package.json');

module.exports = {
  [`GET ${apiPrefix}/app`]: packageJSON,
};
