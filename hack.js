"use latest";

const {Client, Pool} = require('pg');
let pool = null;
let schemaExists = false;
// We only want a single connection


module.exports = function (context, callback) {
  console.log(process.env);

  if (!context.data.ArtistName) {
    return callback(new Error('ArtistName query string paramater is required'));
  }

  let artist = context.data.ArtistName;

  // Setup connection if not already started
  if (!pool) {
    // I wish PG_URL was an envar
    pool = new Pool({connectionString: context.secrets.PG_URL});
    pool.on('error', (err) => {
      console.log('PG Client Error', error);
      process.exit(1);
    });
  }

  pool.connect().then((client) => {
    if  (!schemaExists) {
      console.log('Not sure if artists table exits, checking and creating if needed');
      let stmt = 'SELECT EXISTS(SELET 1 FROM pg_tables WHERE schemaname = ? AND tablename = ?)';
      let params = ['public', 'artists'];
      return client.query(stmt, params).then((result) => {
        console.log('Creating artists table');
        let table = 'CREATE TABLE artists (name varchar(256) PRIMARY KEY, count integer NOT NULL DEFAULT 0)';
      }).then(() => {
        schemaExists = true;  
        return client;
      });
    }
   
    return client; 
  }).then((client) => {
    return callback(null, 'Updated ' + artist);
  }).catch((err) => {
    return callback(err);
  });
};
