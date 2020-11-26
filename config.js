const dotenv = require('dotenv');
dotenv.config();
module.exports = {
  endpoint: process.env.API_URL,
  port: process.env.PORT,
  connectionString: process.env.IOTHUB_CONNECTION_STRING,
  db_host: process.env.DB_HOST,
  db_user: process.env.DB_USER,
  db_database: process.env.DB_DATABASE,
  db_password: process.env.DB_PASSWORD,
  db_port: process.env.DB_PORT
};