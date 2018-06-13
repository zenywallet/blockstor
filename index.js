var opts = require('config');
var Rpc = require('./Rpc');
var bitcoin = require('bitcoinjs-lib');
var Db = require('./Db');
var ApiServer = require('./ApiServer');
var rpc = new Rpc(opts);
rpc.cb = function(cmd, err, res) {
    if(err) {
        if(cmd == 'getBlockHash') {
            if(err.code == -8 && err.message == 'Block height out of range') {
                return null;
            } else if(err.code == -1 && err.message == 'Block number out of range.') {
                return null;
            }
        }
        console.log(err);
    }
    return res;
}
var db = new Db(opts);
var apiserver = new ApiServer(opts, {db: db});

bitcoin.networks['bitzeny'] = {
    messagePrefix: '\u0018Bitzeny Signed Message:\n',
    bech32: 'sz',
    bip32: {
        public: 0x0488b21e,
        private: 0x0488ade4
    },
    pubKeyHash: 0x51,
    scriptHash: 0x05,
    wif: 0x80
};
var network = bitcoin.networks['bitzeny'];

var aborting = false;
async function abort() {
    console.log("\rabort");
    await db.close();
    process.exit(1);
    return new Promise(function() {});
}

process.on('unhandledRejection', console.dir);

['SIGINT', 'SIGTERM'].forEach(function(evt) {
    process.on(evt, async function() {
        aborting = true;
    });
});

var progress_flag = false;
var progress_stop = false;
var progress_t = null;
function progress_agent() {
    if(progress_stop) {
        return;
    }
    progress_t = setTimeout(function() {
        progress_flag = true;
        progress_agent();
    }, 1000);
}

function progress(msg) {
    if(process.stdout.clearLine) {
        process.stdout.clearLine();
        process.stdout.write("\r" + msg);
    } else {
        console.log(msg);
    }
}

function progress_enabled() {
    progress_stop = false;
    progress_agent();
}

function progress_disabled() {
    clearTimeout(progress_t);
    progress_flag = false;
    progress_stop = true;
    progress('');
}

function timestamp(time) {
    var dt = time ? new Date(time * 1000) : new Date();
    var dt_string = dt.toISOString().slice(0, 19).replace('T', ' ');
    return dt_string;
}

async function txins(tx, txid, txins_cb) {
    await Promise.all(tx.ins.map(async function(input) {
        var in_txid = input.hash.reverse().toString('hex');
        var n = input.index;
        if(n != 0xffffffff) {
            var txout = await db.getTxout(in_txid, n);
            if(!txout) {
                throw('ERROR: Txout not found ' + in_txid + ' ' + n);
            }

            await txins_cb(in_txid, n, txout);
        }
    }));
}

async function txouts(tx, txid, txouts_cb) {
    await Promise.all(tx.outs.map(async function(output, n) {
        var amount = output.value;
        var address = null;
        try {
            address = bitcoin.address.fromOutputScript(output.script, network);
        } catch(ex) {}

        if(address) {
            await txouts_cb(txid, n, amount, [address]);
        } else {
            var addresses = [];
            var chunks = bitcoin.script.decompile(output.script);
            for(var k in chunks) {
                var chunk = chunks[k];
                if(Buffer.isBuffer(chunk) && chunk.length !== 1) {
                    try {
                        address = bitcoin.ECPair.fromPublicKeyBuffer(chunk, network).getAddress();
                        addresses.push(address);
                    } catch(ex) {}
                }
            }
            if(addresses.length > 0) {
                await txouts_cb(txid, n, amount, addresses);
            } else {
                address = '#' + txid + '-' + n;
                await txouts_cb(txid, n, amount, [address]);
            }
        }
    }));
}

async function txs_parser(block, txs_cb, txins_cb, txouts_cb) {
    var txids = [];
    await Promise.all(block.transactions.map(async function(tx, i) {
        var txid = tx.getId();
        txids[i] = txid;
        await txs_cb(txid);
        await txouts(tx, txid, txouts_cb);
    }));

    await Promise.all(block.transactions.map(async function(tx, i) {
        await txins(tx, txids[i], txins_cb);
    }));
}

async function txs_rollback_parser(block, txs_cb, txins_cb, txouts_cb) {
    var txids = [];
    await Promise.all(block.transactions.map(async function(tx, i) {
        var txid = tx.getId();
        txids[i] = txid;
        await txins(tx, txid, txins_cb);
    }));

    await Promise.all(block.transactions.map(async function(tx, i) {
        var txid = txids[i];
        await txouts(tx, txid, txouts_cb);
        await txs_cb(txid);
    }));
}

async function txs_parser_log(height, status, mode) {
    var blockcount = await rpc.getBlockCount();
    if(blockcount != null) {
        var msg = timestamp() + ' height(blockstor/coind)=' + height + '/' + blockcount
            + ' delay=' + (blockcount - height) + (status ? ' - ' + status: '');
        if(mode) {
            progress(msg);
        } else {
            console.log(msg);
        }
    }
}

var height = 0;
var prev_hash = null;

async function block_check() {
    if(height > 0) {
        var [hash, db_hash] = await Promise.all([rpc.getBlockHash(height), db.getBlockHash(height)]);
        if(hash == null || db_hash == null) {
            throw('ERROR: getBlockHash');
        }

        while(hash != db_hash.hash) {
            console.log('rollback height=' + height);

            var rawblock = await rpc.getBlock(db_hash.hash, false);
            if(!rawblock) {
                throw('ERROR: getBlock');
            }
            var block = bitcoin.Block.fromHex(rawblock);

            await txs_rollback_parser(block,
                async function txs(txid) {
                },
                async function txins(txid, n, txout) {
                    for(var i in txout.addresses) {
                        var address = txout.addresses[i];
                        await db.setUnspent(address, txid, n, txout.value);
                    }
                },
                async function txouts(txid, n, amount, addresses) {
                    for(var i in addresses) {
                        var address = addresses[i];
                        await db.delUnspent(address, txid, n);
                    }

                    await db.delTxout(txid, n);
                }
            );

            await db.delBlockHash(height);

            if(height > 0) {
                height--;
            } else {
                break;
            }

            [hash, db_hash] = await Promise.all([rpc.getBlockHash(height), db.getBlockHash(height)]);
            if(hash == null || db_hash == null) {
                throw('ERROR: getBlockHash');
            }
        }
    } else {
        height = 0;
        hash = await rpc.getBlockHash(height);
        if(hash == null) {
            throw('ERROR: getBlockHash');
        }
    }
    prev_hash = height ? hash : null;
}

async function block_sync(suppress) {
    var hash = await rpc.getBlockHash(height > 0 ? ++height : 0);

    while(hash && !aborting) {
        var rawblock = await rpc.getBlock(hash, false);
        if(!rawblock) {
            throw('ERROR: getBlock');
        }

        var block = bitcoin.Block.fromHex(rawblock);
        var block_prevHash = block.prevHash.reverse().toString('hex');
        if(prev_hash == block_prevHash || prev_hash == null) {
            var time = block.timestamp;
            await db.setBlockHash(height, hash, time);

            function progress_out() {
                if(suppress && progress_flag) {
                    progress_flag = false;
                    txs_parser_log(height, timestamp(time), true);
                }
            }

            await txs_parser(block,
                async function txs(txid) {
                },
                async function txins(txid, n, txout) {
                    for(var i in txout.addresses) {
                        var address = txout.addresses[i];
                        await db.delUnspent(address, txid, n);
                    }
                    progress_out();
                },
                async function txouts(txid, n, amount, addresses) {
                    await db.setTxout(txid, n, amount, addresses);

                    for(var i in addresses) {
                        var address = addresses[i];
                        await db.setUnspent(address, txid, n, amount);
                    }
                    progress_out();
                }
            );

            if(!suppress) {
                console.log(timestamp() + ' #' + height + ' ' + hash + ' ' + timestamp(time));
            }
            prev_hash = hash;
        } else {
            block_check();
        }

        hash = await rpc.getBlockHash(++height);
    }


    if(!hash && height > 0) {
        height--;
    }
}

;(async function() {
    try {
        height = await db.getLastBlockHeight() || 0;
        await txs_parser_log(height, 'start');
        await block_check();
        progress_enabled();
        await block_sync(true);
        progress_disabled();
        await txs_parser_log(height, aborting ? 'aborted' : 'synced');
        if(aborting) {
            await abort();
        }
    } catch(ex) {
        console.log(ex);
        await abort();
    }

    async function worker() {
        try {
            await block_check();
            await block_sync();

            if(aborting) {
                await abort();
            } else {
                setTimeout(function() {
                    worker();
                }, 1000);
            }
        } catch(ex) {
            console.log(ex);
            await abort();
        }
    }

    worker();
    apiserver.start();
})();
