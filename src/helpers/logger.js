const pino = require('pino')

// define the debug level (output in production and dev)
const logger = pino({
  prettyPrint: true,
  level: process.env.NODE_ENV === 'production' ? 'info' : 'debug'
})

module.exports = {
  logger
}
