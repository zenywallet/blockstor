var express = require('express');
var bodyParser = require('body-parser');

function ApiServer(opts, libs) {
    var db = libs.db;
    var mempool = libs.mempool;
    var app;

    this.start = function() {
        app = express();

        app.use(bodyParser.json({limit: "50mb"}));
        app.use(bodyParser.urlencoded({limit: "50mb", extended: true, parameterLimit: 50000}));
        var router = express.Router();

        async function get_addr(address) {
            var utxos = await db.getUnspent(address);
            var unconfs = mempool.unconfs(address);
            var balance = 0;
            var unconf = 0;
            var unconf_out = 0;
            var unconf_in = 0;
            var utxo_count = 0;

            var txids = {};
            if(utxos.length > 0) {
                for(var i in utxos) {
                    var utxo = utxos[i];
                    balance += utxo.value;
                    txids[utxo.txid] = 1;
                    utxo_count++;
                }
            }

            if(unconfs.txouts) {
                var mempool_txouts = unconfs.txouts;
                mempool_txouts = mempool_txouts.filter(function(txout) {
                    return !txids[txout.txid];
                });
                for(var i in mempool_txouts) {
                    var txout = mempool_txouts[i];
                    unconf_in += txout.value;
                    txids[txout.txid] = 1;
                    utxo_count++;
                }
            }

            if(unconfs.spents) {
                var mempool_spents = unconfs.spents;
                mempool_spents = mempool_spents.filter(function(spent) {
                    return txids[spent.txid];
                });
                for(var i in mempool_spents) {
                    var spent = mempool_spents[i];
                    unconf_out -= spent.value;
                    utxo_count--;
                }
            }

            unconf = unconf_in + unconf_out;

            return {balance: balance, unconf: unconf, unconf_out: unconf_out, unconf_in: unconf_in, utxo_count: utxo_count};
        }

        async function get_utxos(address) {
            var utxos = await db.getUnspent(address);
            var unconfs = mempool.unconfs(address);

            if(unconfs.txouts) {
                var txids = {};
                for(var i in utxos) {
                    var utxo = utxos[i];
                    txids[utxo.txid] = 1;
                }

                var mempool_txouts = unconfs.txouts;
                mempool_txouts = mempool_txouts.filter(function(txout) {
                    return !txids[txout.txid];
                });
                for(var i in mempool_txouts) {
                    var txout = mempool_txouts[i];
                    txids[txout.txid] = 1;
                }

                utxos = utxos.concat(mempool_txouts);
            }

            if(unconfs.spents) {
                var mempool_spent_txids = {};
                for(var i in unconfs.spents) {
                    var spent = unconfs.spents[i];
                    mempool_spent_txids[spent.txid] = 1;
                }

                utxos = utxos.filter(function(utxo) {
                    return !mempool_spent_txids[utxo.txid];
                });
            }

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
