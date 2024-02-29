const sql = require('mssql');

const config = {
  user: 'sa',
  password: '123BIL@l789',
  server: 'localhost',
  database: 'moviehub',
  port: 1433,
  options: {
    enableArithAbort: true,
    trustServerCertificate: true,
  },
};

let pool;

const connectToDatabase = async () => {
  if (!pool) {
    pool = await sql.connect(config);
    console.log('Connected to the database');
  }
  return pool;
};

const closeDatabaseConnection = async () => {
  if (pool) {
    await pool.close();
    console.log('Database connection closed');
  }
};

module.exports = {
  connectToDatabase,
  closeDatabaseConnection,
};
