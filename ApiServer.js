var express = require('express');
var bodyParser = require('body-parser');

function ApiServer(opts, libs) {
    var db = libs.db;
    var app;

    this.start = function() {
        app = express();

        app.use(bodyParser.json({limit: "50mb"}));
        app.use(bodyParser.urlencoded({limit: "50mb", extended: true, parameterLimit: 50000}));
        var router = express.Router();

        async function get_addr(address) {
            var utxos = await db.getUnspent(address);
            var balance = 0;
            for(var i in utxos) {
                var utxo = utxos[i];
                balance += utxo.value;
            }
            return {balance: balance};
        }

        async function get_utxos(address) {
            var utxos = await db.getUnspent(address);
            return utxos;
        }

        // GET - /addr/{addr}
        router.get('/addr/:addr', async function(req, res) {
            res.json({err: 0, res: await get_addr(req.params.addr)});
        });

        // POST - {addrs: [addr1, addr2, ..., addrN]}
        router.post('/addrs', async function(req, res) {
            var addrs = req.body.addrs;
            console.log(addrs);
            var balances = [];
            for(var i in addrs) {
                var addr = addrs[i];
                balances.push(await get_addr(addr));
            }
            res.json({err: 0, res: balances});
        });

        // GET - /utxo/{addr}
        router.get('/utxo/:addr', async function(req, res) {
            res.json({err: 0, res: await get_utxos(req.params.addr)});
        });

        // POST - {addrs: [addr1, addr2, ..., addrN]}
        router.post('/utxos', async function(req, res) {
            var addrs = req.body.addrs;
            var utxos = [];
            for(var i in addrs) {
                var addr = addrs[i];
                utxos.push(await get_utxos(addr));
            }
            res.json({err: 0, res: utxos});
        });

        app.use('/api', router);
        app.use(function(err, req, res, next) {
            res.send({err:1, res: err.message});
        });

        app.listen(opts.server.http_port);
    }
}

module.exports = ApiServer;
