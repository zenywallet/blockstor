var express = require('express');
var bodyParser = require('body-parser');
var UINT64 = require('cuint').UINT64;
var mutexify = require('mutexify');
var cors = require('cors');

function ApiServer(opts, libs) {
    var db = libs.db;
    var mempool = libs.mempool;
    var marker = libs.marker;
    var rpc = libs.rpc;
    var bitcoin = libs.bitcoin;
    var network = libs.network;
    var lock = mutexify();
    var lock_count = 0;
    var lock_maxcount = 5;
    var self = this;
    var app;

    var error_code = {
        SUCCESS: 0,
        ERROR: 1,
        SYNCING: 2,
        ROLLBACKING: 3,
        ROLLBACKED: 4,
        UNKNOWN_APIKEY: 5,
        BUSY: 6,
        TOO_MANY: 7,
        TOO_HIGH: 8
    };

    this.status_code = {
        SYNCED: 0,
        SYNCING: 2,
        ROLLBACKING: 3
    }

    var errval = self.status_code.SYNCING;
    var status_sync = self.status_code.SYNCING;

    this.set_status = function(status_code) {
        errval = status_code;
        status_sync = status_code;
    }

    var height = null;

    this.set_height = function(true_height) {
        height = true_height;
    }

    var tx_sequence = 0;

    this.set_tx_sequence = function(true_tx_sequence) {
        tx_sequence = true_tx_sequence;
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

        function utxo_query_filter(query) {
            var options = {};
            if(query.gte != null) {
                options.gte = parseInt(query.gte);
            } else if(query.gt != null) {
                options.gt = parseInt(query.gt);
            }
            if(query.lte != null) {
                options.lte = parseInt(query.lte);
            } else if(query.lt != null) {
                options.lt = parseInt(query.lt);
            }
            if(query.limit != null) {
                options.limit = parseInt(query.limit);
            }
            if(query.seqbreak != null) {
                options.seqbreak = parseInt(query.seqbreak);
            }
            if(query.reverse != null) {
                options.reverse = parseInt(query.reverse);
            }
            if(query.unconf != null) {
                options.unconf = parseInt(query.unconf);
            }
            return Object.keys(options).length > 0 ? options : null;
        }

        function addrlog_query_filter(query) {
            var options = {};
            if(query.gte != null) {
                options.gte = parseInt(query.gte);
            } else if(query.gt != null) {
                options.gt = parseInt(query.gt);
            }
            if(query.lte != null) {
                options.lte = parseInt(query.lte);
            } else if(query.lt != null) {
                options.lt = parseInt(query.lt);
            }
            if(query.limit != null) {
                options.limit = parseInt(query.limit);
            }
            if(query.seqbreak != null) {
                options.seqbreak = parseInt(query.seqbreak);
            }
            if(query.reverse != null) {
                options.reverse = parseInt(query.reverse);
            }
            return Object.keys(options).length > 0 ? options : null;
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
                        unconf_out.add(spent.value);
                        utxo_count--;
                    }
                }

                if(unconfs.warning) {
                    return {
                        balance: conv_uint64(balance),
                        utxo_count: utxo_count,
                        unconf: {out: conv_uint64(unconf_out), in: conv_uint64(unconf_in), warning: nconfs.warning}
                    };
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

        async function get_utxos(address, options) {
            var utxos = await db.getUnspents(address, options);
            var unconfs = mempool.unconfs(address);

            if(unconfs.txouts && options.unconf) {
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

        async function get_addrlogs(address, options) {
            var addrlogs = await db.getAddrlogs(address, options);
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
            res.json({err: errval, res: await get_utxos(req.params.addr, utxo_query_filter(req.query))});
        });

        // POST - {addrs: [addr1, addr2, ..., addrN]}
        router.post('/utxos', async function(req, res) {
            var addrs = req.body.addrs;
            var utxos = [];
            for(var i in addrs) {
                utxos.push(await get_utxos(addrs[i], utxo_query_filter(req.query)));
            }
            res.json({err: errval, res: utxos});
        });

        // GET - /addrlog/{addr}
        router.get('/addrlog/:addr', async function(req, res) {
            res.json({err: errval, res: await get_addrlogs(req.params.addr, addrlog_query_filter(req.query))});
        });

        // POST - {addrs: [addr1, addr2, ..., addrN]}
        router.post('/addrlogs', async function(req, res) {
            var addrs = req.body.addrs;
            var multilogs = [];
            for(var i in addrs) {
                multilogs.push(await get_addrlogs(addrs[i], addrlog_query_filter(req.query)));
            }
            res.json({err: errval, res: multilogs});
        });

        // GET - /mempool
        router.get('/mempool', async function(req, res) {
            res.json({err: errval, res: mempool.stream_unconfs()});
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
                    if(!check_marker.rollback) {
                        if(sequence > tx_sequence) {
                            await db.setMarker(apikey, sequence, 0);
                            res.json({err: error_code.TOO_HIGH, res: tx_sequence});
                        } else {
                            await db.setMarker(apikey, sequence, 0);
                            res.json({err: error_code.SUCCESS});
                        }
                    } else if(check_marker.sequence == sequence) {
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

        // GET - /height
        router.get('/height', async function(req, res) {
            res.json({err: height == null ? error_code.ERROR : error_code.SUCCESS, res: height});
        });

        // POST - {rawtx: rawtx}
        router.post('/send', async function(req, res) {
            if(lock_count >= lock_maxcount) {
                res.json({err: error_code.BUSY});
                console.log('\rWARNING: send tx busy');
                return;
            }

            lock_count++;
            lock(async function(release) {
                var rawtx = req.body.rawtx;
                var ret_rawtx = await rpc.sendRawTransaction(rawtx);
                release(function() {
                    lock_count--;
                    if(ret_rawtx.code) {
                        res.json({err: error_code.ERROR, res: ret_rawtx});
                        console.log('\rERROR: sendRawTransaction code=' + ret_rawtx.code + ' message=' + ret_rawtx.message);
                    } else {
                        res.json({err: error_code.SUCCESS, res: ret_rawtx});
                        console.log('\rINFO: sendRawTransaction txid=' + ret_rawtx);
                    }
                });
            });
        });

        function get_script_addresses(script, network) {
            try {
                var address = bitcoin.address.fromOutputScript(script, network);
                return [address];
            } catch(ex) {}

            var addresses = [];
            var chunks = bitcoin.script.decompile(script);
            for(var k in chunks) {
                var chunk = chunks[k];
                if(Buffer.isBuffer(chunk) && chunk.length !== 1) {
                    try {
                        var address = bitcoin.payments.p2pkh({ pubkey: chunk, network: network }).address;
                        addresses.push(address);
                    } catch(ex) {}
                }
            }
            if(addresses.length > 0) {
                return addresses;
            }
            return null;
        }

        // GET - /tx/{txid}
        router.get('/tx/:txid', async function(req, res) {
            if(lock_count >= lock_maxcount) {
                res.json({err: error_code.BUSY});
                console.log('\rWARNING: get tx busy');
                return;
            }

            lock_count++;
            lock(async function(release) {
                var txid = req.params.txid;
                var ret_rawtx = await rpc.getRawTransaction(txid);
                var rawblock = null;
                if(ret_rawtx.code && !opts.db.rawblocks) {
                    var dbtx = await db.getTx(txid);
                    if(dbtx) {
                        var height = dbtx.height;
                        var db_hash = await db.getBlockHash(height);
                        rawblock = await rpc.getBlock(db_hash.hash, false);
                    }
                }
                release(async function() {
                    lock_count--;
                    if(ret_rawtx.code && rawblock == null) {
                        if(opts.db.rawblocks) {
                            var dbtx = await db.getTx(txid);
                            if(dbtx) {
                                var height = dbtx.height;
                                var db_hash = await db.getBlockHash(height);
                                rawblock = await db.getRawBlock(height, db_hash.hash);
                            }
                            if(!rawblock) {
                                res.json({err: error_code.ERROR, res: ret_rawtx});
                                console.log('\rERROR: getRawTransactrion code=' + ret_rawtx.code + ' message=' + ret_rawtx.message + ' txid=' + txid + ' fallback failed');
                                return;
                            }
                        } else {
                            res.json({err: error_code.ERROR, res: ret_rawtx});
                            console.log('\rERROR: getRawTransactrion code=' + ret_rawtx.code + ' message=' + ret_rawtx.message + ' txid=' + txid);
                            return;
                        }
                    }

                    var tx = null;
                    if(rawblock) {
                        var block = bitcoin.Block.fromHex(rawblock);
                        for(var i in block.transactions) {
                            var block_tx = block.transactions[i];
                            if(block_tx.getId() == txid) {
                                tx = block_tx;
                                break;
                            }
                        }
                        if(!tx) {
                            res.json({err: error_code.ERROR, res: ret_rawtx});
                            console.log('\rERROR: getRawTransactrion code=' + ret_rawtx.code + ' message=' + ret_rawtx.message + ' txid=' + txid + ' fallback failed');
                            return;
                        }
                    }
                    tx = tx || bitcoin.Transaction.fromHex(ret_rawtx);
                    var ret_tx = {ins: [], outs: []};
                    var fee = UINT64(0);
                    var reward = false;
                    for(var i in tx.ins) {
                        var in_txid = Buffer.from(tx.ins[i].hash).reverse().toString('hex');
                        var n = tx.ins[i].index;
                        if(n != 0xffffffff) {
                            var txout = await db.getTxout(in_txid, n);
                            if(!txout) {
                                console.log('\rERROR: Txout not found ' + in_txid + ' ' + n);
                                res.json({err: error_code.ERROR, res: 'Txout not found ' + in_txid + ' ' + n});
                            }
                            ret_tx.ins.push({value: conv_uint64(txout.value), addrs: txout.addresses});
                            fee.add(txout.value);
                        } else {
                            reward = true;
                        }
                    }
                    if(reward) {
                        for(var i in tx.outs) {
                            ret_tx.outs.push({value: conv_uint64(tx.outs[i].value), addrs: get_script_addresses(tx.outs[i].script, network) || []});
                        }
                    } else {
                        for(var i in tx.outs) {
                            ret_tx.outs.push({value: conv_uint64(tx.outs[i].value), addrs: get_script_addresses(tx.outs[i].script, network) || []});
                            fee.subtract(tx.outs[i].value);
                        }
                    }
                    ret_tx.fee = conv_uint64(fee);
                    res.json({err: errval, res: ret_tx});
                    console.log('\rINFO: getRawTransactrion txid=' + txid);
                });
            });
        });

        // GET - /search/{keyword}
        router.get('/search/:keyword', async function(req, res) {
            var keyword = req.params.keyword;
            var s_addrs = await db.searchAddresses(keyword);
            if(s_addrs === null) {
                res.json({err: error_code.TOO_MANY});
                return;
            }
            var s_txids;
            if((keyword.match(/([0-9]|[a-f])/gim) || []).length === keyword.length) {
                s_txids = await db.searchTxids(keyword);
                if(s_txids === null) {
                    res.json({err: error_code.TOO_MANY});
                    return;
                }
            }
            res.json({err: error_code.SUCCESS, res: {addrs: s_addrs || [], txids: s_txids || []}});
        });

        // GET - /status
        router.get('/status', async function(req, res) {
            res.json({err: error_code.SUCCESS, res: {sync: status_sync}});
        });

        app.use(cors());
        app.use(opts.server.http_path || '/api', router);
        app.use(function(err, req, res, next) {
            res.send({err: error_code.ERROR, res: err.message});
        });

        if(opts.server.http_port == opts.server.ws_port) {
            return app;
        }

        app.listen(opts.server.http_port);
        return null;
    }
}

module.exports = ApiServer;
