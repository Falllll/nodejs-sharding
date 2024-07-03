const app = require('express')();
const {Client} = require('pg');
const crypto = require('crypto');
const HashRing = require('hashring');
const hr = new HashRing();
hr.add(['5432', '5433', '5434']);

const clients = {
    '5432' : new Client({
        user: 'postgres',
        host: 'localhost',
        database: 'postgres',
        password: 'password',
        port: '5432',
    }),
    '5433' : new Client({
        user: 'postgres',
        host: 'localhost',
        database: 'postgres',
        password: 'password',
        port: '5433',
    }),
    '5434' : new Client({
        user: 'postgres',
        host: 'localhost',
        database: 'postgres',
        password: 'password',
        port: '5434',
    }),
}
async function connect() {
    try {
        await clients['5432'].connect();
        await clients['5433'].connect();
        await clients['5434'].connect();
        console.log('All clients connected successfully');
    } catch (e) {
        console.error(`Failed to connect: ${e.message}`);
    }
}
connect();


app.get('/:urlId', async(req, res) => {
    //https://www.wikipedia.com/yh3j4

    const urlId = req.params.urlId;
    const server = hr.get(urlId);
    const result = await clients[server].query('SELECT * FROM urls WHERE url_id = $1', [urlId]);

    if(result.rowCount > 0) {
        res.send({
            'urlId': urlId,
            'url': result.rows[0],
            'server': server, 
        })
    }else
        res.sendStatus(404);
    
});

app.post('/', async (req, res) => {
    const url = req.query.url;
    // www.wikipedia.com/sharding
    // consistently hash the url to get the port number
    const hash = crypto.createHash('sha256').update(url).digest('base64');
    const urlId = hash.substring(0, 5);

    const server = hr.get(urlId);

    await clients[server].query('INSERT INTO urls (url, url_id) VALUES ($1,$2)', [url, urlId]);

    res.send({
        'hash': hash,
        'urlId': urlId,
        'url': url,
        'server': server, 
    })
});

app.listen(8081, () => console.log('Server started on http://localhost:8081'));