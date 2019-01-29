var opts = require('config');
var Rpc = require('./Rpc');
var Tcp = require('./Tcp', opts);
var bitcoin = require('./BitcoinjsExt')(require('bitcoinjs-lib'), opts);
var UINT64 = require('cuint').UINT64;
var Db = require('./Db');
var MemPool = require('./MemPool');
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
var network = bitcoin.networks[opts.target_network];
var mempool = new MemPool(opts, {bitcoin: bitcoin, rpc: rpc, db: db, network: network});
var apiserver = new ApiServer(opts, {db: db, mempool: mempool});

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

function now() {
    return Math.floor(new Date().getTime() / 1000);
}

var height = 0;
var prev_hash = null;
var tx_sequence = 0;

function addrvals_aggregate(addrvals) {
    var agg = {};
    for(var i in addrvals) {
        var addrval = addrvals[i];
        if(!agg[addrval.address]) {
            agg[addrval.address] = {value: addrval.value, utxo_count: 1};
        } else {
            agg[addrval.address].value.add(addrval.value);
            agg[addrval.address].utxo_count++;
        }
    }
    var addrs = [];
    for(var i in agg) {
        addrs.push({address: i, value: agg[i].value, utxo_count: agg[i].utxo_count});
    }
    return addrs;
}

async function txins(tx, txid, sequence, txins_cb) {
    var addrvals = [];
    await Promise.all(tx.ins.map(async function(input) {
        var in_txid = Buffer.from(input.hash).reverse().toString('hex');
        var n = input.index;
        if(n != 0xffffffff) {
            var txout = await db.getTxout(in_txid, n);
            if(!txout) {
                throw('ERROR: Txout not found ' + in_txid + ' ' + n);
            }

            await txins_cb(in_txid, n, sequence, txout);
            for(var i in txout.addresses) {
                addrvals.push({address: txout.addresses[i], value: txout.value});
            }
        }
    }));

    return addrvals_aggregate(addrvals);
}

async function txouts(tx, txid, sequence, txouts_cb) {
    var addrvals = [];
    await Promise.all(tx.outs.map(async function(output, n) {
        var amount = output.value;
        var address = null;
        try {
            address = bitcoin.address.fromOutputScript(output.script, network);
        } catch(ex) {}

        if(address) {
            await txouts_cb(txid, n, sequence, amount, [address]);
            addrvals.push({address: address, value: amount});
        } else {
            var addresses = [];
            var chunks = bitcoin.script.decompile(output.script);
            for(var k in chunks) {
                var chunk = chunks[k];
                if(Buffer.isBuffer(chunk) && chunk.length !== 1) {
                    try {
                        address = bitcoin.payments.p2pkh({ pubkey: chunk, network: network }).address;
                        addresses.push(address);
                    } catch(ex) {}
                }
            }
            if(addresses.length > 0) {
                await txouts_cb(txid, n, sequence, amount, addresses);
                for(var i in addresses) {
                    addrvals.push({address: addresses[i], value: amount});
                }
            } else {
                address = '@' + txid + '-' + n;
                await txouts_cb(txid, n, sequence, amount, [address]);
                addrvals.push({address: address, value: amount});
            }
        }
    }));

    return addrvals_aggregate(addrvals);
}

async function txs_parser(block, txs_cb, txins_cb, txouts_cb, addrins_cb, addrouts_cb) {
    var txids = [];
    var sequences = [];
    var addrins = [];
    var addrouts = [];
    await Promise.all(block.transactions.map(async function(tx, i) {
        var txid = tx.getId();
        txids[i] = txid;
        var sequence = tx_sequence + i;
        sequences[i] = sequence;
        await txs_cb(txid, sequence);
        addrouts[i] = await txouts(tx, txid, sequence, txouts_cb);
    }));

    await Promise.all(block.transactions.map(async function(tx, i) {
        addrins[i] = await txins(tx, txids[i], sequences[i], txins_cb);
    }));

    for(var i in addrouts) {
        var addrout = addrouts[i];
        for(var j in addrout) {
            await addrouts_cb(txids[i], sequences[i], addrout[j]);
        }
    }
    for(var i in addrins) {
        var addrin = addrins[i];
        for(var j in addrin) {
            await addrins_cb(txids[i], sequences[i], addrin[j]);
        }
    }
    tx_sequence += block.transactions.length;
}

async function txs_rollback_parser(block, txs_cb, txins_cb, txouts_cb, addrins_cb, addrouts_cb) {
    var txids = [];
    var sequences = [];
    var addrins = [];
    var addrouts = [];
    tx_sequence -= block.transactions.length;
    await Promise.all(block.transactions.map(async function(tx, i) {
        var txid = tx.getId();
        txids[i] = txid;
        var sequence = tx_sequence + i;
        sequences[i] = sequence;
        addrins[i] = await txins(tx, txid, sequence, txins_cb);
    }));

    await Promise.all(block.transactions.map(async function(tx, i) {
        var txid = txids[i];
        addrouts[i] = await txouts(tx, txid, sequences[i], txouts_cb);
        await txs_cb(txid, sequences[i]);
    }));

    for(var i in addrins) {
        var addrin = addrins[i];
        for(var j in addrin) {
            await addrins_cb(txids[i], sequences[i], addrin[j]);
        }
    }
    for(var i in addrouts) {
        var addrout = addrouts[i];
        for(var j in addrout) {
            await addrouts_cb(txids[i], sequences[i], addrout[j]);
        }
    }
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
    } else {
        throw('ERROR: getBlockCount');
    }
}

async function block_writer(block, hash, time, rawblock) {
    if(opts.db.rawblocks) {
        await db.setRawBlock(height, hash, rawblock, now());
    }
    await db.setBlockHash(height, hash, time, tx_sequence);

    await txs_parser(block,
        async function txs(txid, sequence) {
            await db.setTx(txid, height, time, sequence);
        },
        async function txins(txid, n, sequence, txout) {
            for(var i in txout.addresses) {
                var address = txout.addresses[i];
                await db.delUnspent(address, txout.sequence, txid, n);
            }
        },
        async function txouts(txid, n, sequence, amount, addresses) {
            await db.setTxout(txid, n, sequence, amount, addresses);

            for(var i in addresses) {
                var address = addresses[i];
                await db.setUnspent(address, sequence, txid, n, amount);
            }
        },
        async function addrins(txid, sequence, addrval) {
            var val = await db.getAddrval(addrval.address);
            if(val == null) {
                throw('ERROR: Address not found ' + addrval.address);
            }

            await db.setAddrval(addrval.address, val.value.subtract(addrval.value), val.utxo_count - addrval.utxo_count);
            await db.setAddrlog(addrval.address, sequence, 0, txid, addrval.value);
        },
        async function addrouts(txid, sequence, addrval) {
            var val = await db.getAddrval(addrval.address);
            val = val ? val : {value: UINT64(0), utxo_count: 0};

            await db.setAddrval(addrval.address, val.value.add(addrval.value), val.utxo_count + addrval.utxo_count);
            await db.setAddrlog(addrval.address, sequence, 1, txid, addrval.value);
        }
    );
}

async function block_rewriter(block, hash, time, rawblock) {
    await txs_parser(block,
        async function txs(txid, sequence) {
            await db.setTx(txid, height, time, sequence);
        },
        async function txins(txid, n, sequence, txout) {
            for(var i in txout.addresses) {
                var address = txout.addresses[i];
                await db.delUnspent(address, txout.sequence, txid, n);
            }
        },
        async function txouts(txid, n, sequence, amount, addresses) {
            await db.setTxout(txid, n, sequence, amount, addresses);

            for(var i in addresses) {
                var address = addresses[i];
                await db.setUnspent(address, sequence, txid, n, amount);
            }
        },
        async function addrins(txid, sequence, addrval) {
            var val = UINT64(0);
            var utxo_count = 0;
            var utxos = await db.getUnspents(addrval.address);
            for(var i in utxos) {
                val.add(utxos[i].value);
                utxo_count++;
            }
            await db.setAddrval(addrval.address, val, utxo_count);
            await db.setAddrlog(addrval.address, sequence, 0, txid, addrval.value);
        },
        async function addrouts(txid, sequence, addrval) {
            var val = UINT64(0);
            var utxo_count = 0;
            var utxos = await db.getUnspents(addrval.address);
            for(var i in utxos) {
                val.add(utxos[i].value);
                utxo_count++;
            }
            await db.setAddrval(addrval.address, val, utxo_count);
            await db.setAddrlog(addrval.address, sequence, 1, txid, addrval.value);
        }
    );
}

async function block_rollback(block, hash) {
    await txs_rollback_parser(block,
        async function txs(txid, sequence) {
        },
        async function txins(txid, n, sequence, txout) {
            for(var i in txout.addresses) {
                var address = txout.addresses[i];
                await db.setUnspent(address, sequence, txid, n, txout.value);
            }
        },
        async function txouts(txid, n, sequence, amount, addresses) {
            for(var i in addresses) {
                var address = addresses[i];
                await db.delUnspent(address, sequence, txid, n);
            }

            await db.delTxout(txid, n);
        },
        async function addrins(txid, sequence, addrval) {
            var val = await db.getAddrval(addrval.address);
            if(val == null) {
                throw('ERROR: Address not found ' + addrval.address);
            }

            await db.delAddrlog(addrval.address, sequence, 0);
            await db.setAddrval(addrval.address, val.value.subtract(addrval.value), val.utxo_count + addrval.utxo_count);
        },
        async function addrouts(txid, sequence, addrval) {
            var val = await db.getAddrval(addrval.address);
            if(val == null) {
                throw('ERROR: Address not found ' + addrval.address);
            }

            await db.delAddrlog(addrval.address, sequence, 1);
            var exist = await db.checkAddrlogExist(addrval.address);
            if(exist) {
                await db.setAddrval(addrval.address, val.value.subtract(addrval.value), val.utxo_count - addrval.utxo_count);
            } else {
                await db.delAddrval(addrval.address);
            }
        }
    );

    await db.delBlockHash(height);
    if(opts.db.rawblocks && opts.db.remove_orphan_rawblocks) {
        await db.deleteRawBlock(height, hash);
    }
}

async function get_rawblock(height, hash) {
    var rawblock;
    if(opts.db.rawblocks) {
        rawblock = await db.getRawBlock(height, hash);
        if(!rawblock) {
            throw('ERROR: getRawBlock ' + height + ' ' + hash);
        }
    } else {
        rawblock = await rpc.getBlock(hash, false);
        if(!rawblock) {
            throw('ERROR: getBlock ' + hash);
        }
    }
    return rawblock;
}

async function block_check() {
    if(height > 0) {
        var [hash, db_hash] = await Promise.all([rpc.getBlockHash(height), db.getBlockHash(height)]);
        if(hash == null || db_hash == null) {
            throw('ERROR: getBlockHash');
        }

        while(hash != db_hash.hash && !aborting) {
            console.log('rollback height=' + height);

            var rawblock = await get_rawblock(height, db_hash.hash);
            var block = bitcoin.Block.fromHex(rawblock);
            await block_rollback(block, db_hash.hash);

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

async function lastblock_rewrite() {
    var last = await db.getLastBlockHeight();
    if(last) {
        var db_hash = await db.getBlockHash(last);
        var rawblock = await get_rawblock(last, db_hash.hash);
        var block = bitcoin.Block.fromHex(rawblock);
        tx_sequence = db_hash.sequence;
        await block_rewriter(block, db_hash.hash, block.timestamp, rawblock);
    }
}

async function block_sync(suppress) {
    var new_block = false;
    var hash = await rpc.getBlockHash(height > 0 ? ++height : 0);

    while(hash && !aborting) {
        var rawblock = await rpc.getBlock(hash, false);
        if(!rawblock) {
            throw('ERROR: getBlock');
        }

        var block = bitcoin.Block.fromHex(rawblock);
        var block_prevHash = Buffer.from(block.prevHash).reverse().toString('hex');
        if(prev_hash == block_prevHash || prev_hash == null) {
            var time = block.timestamp;

            await block_writer(block, hash, time, rawblock);

            if(!suppress) {
                console.log("\r" + timestamp() + ' #' + height + ' ' + hash + ' ' + timestamp(time));
            } else if(progress_flag) {
                progress_flag = false;
                txs_parser_log(height, timestamp(time) + ' (RPC mode)', true);
            }
            prev_hash = hash;
        } else {
            block_check();
        }

        hash = await rpc.getBlockHash(++height);
        new_block = true;
    }

    if(!hash && height > 0) {
        height--;
    }

    return new_block;
}

async function block_sync_tcp(suppress) {
    var tcp = new Tcp(opts);

    var hash = await rpc.getBlockHash(height);
    if(!hash) {
        throw('ERROR: getBlockHash');
    }
    var rawblock = await rpc.getBlock(hash, false);
    if(!rawblock) {
        throw('ERROR: getBlock');
    }

    var hash_buf = Buffer.from(hash, 'hex').reverse();
    tcp.start(height, hash_buf);

    var blockdata;
    if(height == 0) {
        var block = bitcoin.Block.fromHex(rawblock);
        blockdata = {height: height, hash: hash_buf, block: block, rawblock: rawblock};
    } else {
        blockdata = await tcp.getblock();
    }

    while(blockdata && !aborting) {
        height = blockdata.height;
        hash = Buffer.from(blockdata.hash).reverse().toString('hex');
        var block = blockdata.block;
        var time = block.timestamp;

        await block_writer(block, hash, time, blockdata.rawblock);

        if(suppress && progress_flag) {
            progress_flag = false;
            txs_parser_log(height, timestamp(time) + ' (TCP mode)', true);
        }
        prev_hash = hash;

        blockdata = await tcp.getblock();
    }

    tcp.stop();
}

;(async function() {
    apiserver.start();

    try {
        height = await db.getLastBlockHeight() || 0;
        await txs_parser_log(height, 'start');
        await lastblock_rewrite();
        await block_check();
        progress_enabled();
        await block_sync_tcp(true);
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
            var new_block = await block_sync();
            if(new_block) {
                mempool.update(true);
            } else {
                mempool.update();
            }

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
    apiserver.ready(true);
})();
