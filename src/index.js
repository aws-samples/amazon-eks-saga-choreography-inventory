const logger = require('./utils/logger')
const appConfig = require('./utils/config').getAppConfig()
const snsSub = require('./sns/sub')

if (Object.keys(appConfig).length === 0) {
  logger.error(`Configuration data was not received.`)
  process.exit(1)
}

snsSub.receiveInputMessages(appConfig)