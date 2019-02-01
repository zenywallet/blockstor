var express = require('express');
var bodyParser = require('body-parser');
var UINT64 = require('cuint').UINT64;

function ApiServer(opts, libs) {
    var db = libs.db;
    var mempool = libs.mempool;
    var marker = libs.marker;
    var self = this;
    var app;

    var error_code = {
        SUCCESS: 0,
        ERROR: 1,
        SYNCING: 2,
        ROLLBACKING: 3,
        ROLLBACKED: 4,
        UNKNOWN_APIKEY: 5
    }

    this.status_code = {
        SYNCED: 0,
        SYNCING: 2,
        ROLLBACKING: 3
    }

    var errval = self.status_code.SYNCING;

    this.set_status = function(status_code) {
        errval = status_code;
    }

    this.start = function() {
        app = express();

        app.use(bodyParser.json({limit: "50mb"}));
        app.use(bodyParser.urlencoded({limit: "50mb", extended: true, parameterLimit: 50000}));
        var router = express.Router();

        function conv_uint64(uint64_val) {
            strval = uint64_val.toString();
            val = parseInt(strval);
            if(val > Number.MAX_SAFE_INTEGER) {
                return strval;
            }
            return val;
        }

        async function get_addr(address) {
            var unconfs = mempool.unconfs(address);
            if(unconfs.txouts || unconfs.spents) {
                var utxos = await db.getUnspents(address);
                var balance = UINT64(0);
                var unconf_out = UINT64(0);
                var unconf_in = UINT64(0);
                var utxo_count = 0;
                var txids = {};
                if(utxos.length > 0) {
                    for(var i in utxos) {
                        var utxo = utxos[i];
                        balance.add(utxo.value);
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
                        unconf_in.add(txout.value);
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
                        unconf_out.subtract(spent.value);
                        utxo_count--;
                    }
                }
                return {
                    balance: conv_uint64(balance),
                    utxo_count: utxo_count,
                    unconf: {out: conv_uint64(unconf_out), in: conv_uint64(unconf_in)}
                };
            } else {
                var addrval = await db.getAddrval(address);
                if(addrval == null) {
                    return {};
                } else {
                    return {
                        balance: conv_uint64(addrval.value),
                        utxo_count: addrval.utxo_count
                    };
                }
            }
        }

        async function get_utxos(address) {
            var utxos = await db.getUnspents(address);
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

            for(var i in utxos) {
                utxos[i].value = conv_uint64(utxos[i].value);
            }
            return utxos;
        }

        async function get_addrlogs(address) {
            var addrlogs = await db.getAddrlogs(address);
            for(var i in addrlogs) {
                var addrlog = addrlogs[i];
                addrlog.value = conv_uint64(addrlog.value);
                var tx = await db.getTx(addrlog.txid);
                addrlog.height = tx.height;
                addrlog.time = tx.time;
            }
            return addrlogs;
        }

        // GET - /addr/{addr}
        router.get('/addr/:addr', async function(req, res) {
            res.json({err: errval, res: await get_addr(req.params.addr)});
        });

        // POST - {addrs: [addr1, addr2, ..., addrN]}
        router.post('/addrs', async function(req, res) {
            var addrs = req.body.addrs;
            var balances = [];
            for(var i in addrs) {
                balances.push(await get_addr(addrs[i]));
            }
            res.json({err: errval, res: balances});
        });

        // GET - /utxo/{addr}
        router.get('/utxo/:addr', async function(req, res) {
            res.json({err: errval, res: await get_utxos(req.params.addr)});
        });

        // POST - {addrs: [addr1, addr2, ..., addrN]}
        router.post('/utxos', async function(req, res) {
            var addrs = req.body.addrs;
            var utxos = [];
            for(var i in addrs) {
                utxos.push(await get_utxos(addrs[i]));
            }
            res.json({err: errval, res: utxos});
        });

        // GET - /addrlog/{addr}
        router.get('/addrlog/:addr', async function(req, res) {
            res.json({err: errval, res: await get_addrlogs(req.params.addr)});
        });

        // POST - {addrs: [addr1, addr2, ..., addrN]}
        router.post('/addrlogs', async function(req, res) {
            var addrs = req.body.addrs;
            var multilogs = [];
            for(var i in addrs) {
                multilogs.push(await get_addrlogs(addrs[i]));
            }
            res.json({err: errval, res: multilogs});
        });

        // GET - /marker/{apikey}
        router.get('/marker/:apikey', async function(req, res) {
            var apikey = req.params.apikey;
            if(opts.apikeys[apikey]) {
                var sequence = await db.getMarker(apikey) || 0;
                res.json({err: error_code.SUCCESS, res: sequence});
            } else {
                res.json({err: error_code.UNKNOWN_APIKEY});
            }
        });

        // POST - {apikey: apikey, sequence: sequence}
        router.post('/marker', async function(req, res) {
            var apikey = req.body.apikey;
            var sequence = req.body.sequence;
            if(opts.apikeys[apikey]) {
                if(!marker.rollbacking) {
                    var check_marker = await db.getMarker(apikey);
                    if(!check_marker.rollback || check_marker.sequence == sequence) {
                        await db.setMarker(apikey, sequence, 0);
                        res.json({err: error_code.SUCCESS});
                    } else {
                        res.json({err: error_code.ROLLBACKED, res: check_marker.sequence});
                    }
                } else {
                    res.json({err: error_code.ROLLBACKING});
                }
            } else {
                res.json({err: error_code.UNKNOWN_APIKEY});
            }
        });

        app.use('/api', router);
        app.use(function(err, req, res, next) {
            res.send({err: 1, res: err.message});
        });

        app.listen(opts.server.http_port);
    }
}

module.exports = ApiServer;
