"use latest";

const {Client, Pool} = require('pg');
let pool = null;
// Track if we know the schema exists to save queries
let schemaExists = false;

module.exports = function (context, callback) {
  if (!context.data.ArtistName && !context.body_raw) {
    console.log(context);
    return callback(new Error('ArtistName query string paramater is required'));
  }
  let artist = context.data.ArtistName || context.body_raw;

  // Setup connection if not already started
  if (!pool) {
    // I wish PG_URL was an envar
    pool = new Pool({connectionString: context.secrets.PG_URL});
    pool.on('error', (err) => {
      console.log('PG Error', err);
      process.exit(1);
    });
  }

  pool.connect().then((client) => {
    // Setup schema if it does not already exist
    if (!schemaExists) {
      console.log('Not sure if artists table exits, checking and creating if needed');
      let stmt = 'SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = $1 AND tablename = $2)';
      let params = ['public', 'artists'];
      return client.query(stmt, params).then((result) => {
        // If exists we are done and can move on to the upsert
        if (result.rows[0].exists) {
          return true;
        }
        
        console.log('Creating artists table');
        let table = 'CREATE TABLE artists (name varchar(256) PRIMARY KEY, ' +
          'total_count integer NOT NULL DEFAULT 0)';
        return client.query(table);
      }).then(() => {
        // We don't need to check for creation again
        schemaExists = true;

        return upsertArtist(client, artist);
      }).catch((err) => {
        client.release();
        throw err;
      });
    }
   
    return upsertArtist(client, artist).catch((err) => {
      client.release();
      throw err;
    }); 
  }).then((result) => {
    console.log(artist, result.rows);
    return callback(null, 'Updated ' + artist + ' to ' + result.rows[0].total_count);
  }).catch((err) => {
    return callback(err);
  });
};

function upsertArtist(client, artist) {
  let stmt = 'INSERT INTO artists (name, total_count) VALUES ($1, $2) ' +
    'ON CONFLICT (name) DO UPDATE SET total_count = artists.total_count + 1' +
    'RETURNING total_count';
  let params = [artist, 1];
  return client.query(stmt, params);
}
