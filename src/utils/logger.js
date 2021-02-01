const winston = require('winston')
const mom = require('moment-timezone')

let TZ = process.env.TZ || 'Asia/Kolkata'

function getCurrentTimestamp() {
  return mom().tz(TZ).format('YYYY-MM-DD_HH:mm:ss.SSS')
}

const logger = winston.createLogger({
  transports: [
    new winston.transports.Console()
  ],
  format: winston.format.combine(
    winston.format.timestamp({ format: getCurrentTimestamp }), winston.format.json()
  ),
})


function infoLogger(message) {
  logger.info(message)
}

function warnLogger(message) {
  logger.warn(message)
}

function errorLogger(message) {
  logger.error(message)
}

module.exports = {
  info: infoLogger,
  warn: warnLogger,
  error: errorLogger
};