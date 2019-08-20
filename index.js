var opts = require('config');
var Rpc = require('./Rpc');
var Tcp = require('./Tcp', opts);
var bitcoin = require('./BitcoinjsExt')(require('bitcoinjs-lib'), opts);
var UINT64 = require('cuint').UINT64;
var Db = require('./Db');
var MemPool = require('./MemPool');
var Marker = require('./Marker');
var ApiServer = require('./ApiServer');
var ApiStream = require('./ApiStream');
var rpc = new Rpc(opts);
rpc.cb = function(cmd, err, res) {
    if(err) {
        if(cmd == 'getBlockHash') {
            if(err.code == -8 && err.message == 'Block height out of range') {
                return null;
            } else if(err.code == -1 && err.message == 'Block number out of range.') {
                return null;
            }
        } else if(cmd == 'sendRawTransaction' || cmd == 'getRawTransaction') {
            return {code: err.code, message: err.message};
        }
        console.log('\r' + err);
    }
    return res;
}
var db = new Db(opts);
var network = bitcoin.networks[opts.target_network];
var mempool = new MemPool(opts, {bitcoin: bitcoin, rpc: rpc, db: db, network: network});
var marker = new Marker(opts, {db: db});
var apiserver = new ApiServer(opts, {db: db, mempool: mempool, marker: marker, rpc: rpc, bitcoin: bitcoin, network: network});
var apistream = new ApiStream(opts);

var aborting = false;
async function abort() {
    console.log('\rabort');
    await db.close();
    process.exit(1);
    return new Promise(function() {});
}

process.on('unhandledRejection', function(err) {
    console.dir(err);
    abort();
});

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
        process.stdout.write('\r' + msg);
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
                if(address.length > 1) {
                    for(var i in addresses) {
                        addrvals.push({address: addresses[i], value: amount, multi: 1});
                    }
                } else {
                    addrvals.push({address: addresses[0], value: amount});
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
    apiserver.set_tx_sequence(tx_sequence);
}

async function txs_rollback_parser(block, txs_cb, txins_cb, txouts_cb, addrins_cb, addrouts_cb) {
    var txids = [];
    var sequences = [];
    var addrins = [];
    var addrouts = [];
    tx_sequence -= block.transactions.length;
    apiserver.set_tx_sequence(tx_sequence);
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

async function block_writer_without_stream(block, hash, time, rawblock) {
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
            await db.setAddrlog(addrval.address, sequence, addrval.multi ? 2 : 1, txid, addrval.value);
        }
    );
}

function conv_uint64(uint64_val) {
    strval = uint64_val.toString();
    val = parseInt(strval);
    if(val > Number.MAX_SAFE_INTEGER) {
        return strval;
    }
    return val;
}

async function block_writer_with_stream(block, hash, time, rawblock) {
    var stream_data = {height: height, hash: hash, time: time, addrs: {}, txs: {}};

    if(opts.db.rawblocks) {
        await db.setRawBlock(height, hash, rawblock, now());
    }
    await db.setBlockHash(height, hash, time, tx_sequence);

    await txs_parser(block,
        async function txs(txid, sequence) {
            await db.setTx(txid, height, time, sequence);

            stream_data.txs[sequence] = txid;
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

            var balance = val.value.subtract(addrval.value);
            var utxo_count = val.utxo_count - addrval.utxo_count;
            await db.setAddrval(addrval.address, balance, utxo_count);
            await db.setAddrlog(addrval.address, sequence, 0, txid, addrval.value);

            if(addrval.address.charAt(0) != '@') {
                var vals = stream_data.addrs[addrval.address] ? stream_data.addrs[addrval.address].vals : [];
                vals.push({sequence: sequence, type: 0, value: conv_uint64(addrval.value)});
                stream_data.addrs[addrval.address] = {balance: conv_uint64(balance), utxo_count: utxo_count, vals: vals};
            }
        },
        async function addrouts(txid, sequence, addrval) {
            var val = await db.getAddrval(addrval.address);
            val = val ? val : {value: UINT64(0), utxo_count: 0};

            var balance = val.value.add(addrval.value);
            var utxo_count = val.utxo_count + addrval.utxo_count;
            await db.setAddrval(addrval.address, balance, utxo_count);
            await db.setAddrlog(addrval.address, sequence, addrval.multi ? 2 : 1, txid, addrval.value);

            if(addrval.address.charAt(0) != '@') {
                var vals = stream_data.addrs[addrval.address] ? stream_data.addrs[addrval.address].vals : [];
                vals.push({sequence: sequence, type: addrval.multi ? 2 : 1, value: conv_uint64(addrval.value)});
                stream_data.addrs[addrval.address] = {balance: conv_uint64(balance), utxo_count: utxo_count, vals: vals};
            }
        }
    );

    apiserver.set_height(height);
    apistream.send_all(stream_data);
}

var block_writer = block_writer_without_stream;
function enable_block_writer_stream(flag) {
    if(flag) {
        block_writer = block_writer_with_stream;
    } else {
        block_writer = block_writer_without_stream;
    }
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
            await db.setAddrlog(addrval.address, sequence, addrval.multi ? 2 : 1, txid, addrval.value);
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

            await db.delAddrlog(addrval.address, sequence, addrval.multi ? 2 : 1);
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

        if(hash != db_hash.hash && !aborting) {
            apiserver.set_status(apiserver.status_code.ROLLBACKING);
            marker.rollbacking = true;
            do {
                console.log('\rrollback height=' + height);

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
            } while(hash != db_hash.hash && !aborting);
            await marker.rollback_markers(tx_sequence);
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
        apiserver.set_tx_sequence(tx_sequence);
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
                console.log('\r' + timestamp() + ' #' + height + ' ' + hash + ' ' + timestamp(time));
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

    if(marker.rollbacking) {
        marker.rollbacking = false;
        apiserver.set_status(apiserver.status_code.SYNCED);
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

var network_extras = {};
if(network.bech32_extra) {
    for(var i in network.bech32_extra) {
        network_extras[i] = JSON.parse(JSON.stringify(network));
        network_extras[i].bech32 = network.bech32_extra[i];
    }
}
network_extras_enabled = network.bech32 && Object.keys(network_extras).length > 0;

async function update_address_alias(address) {
    var decode = null;
    try {
        decode = bitcoin.address.fromBech32(address);
    } catch(e) {}
    if(decode) {
        for(var i in network_extras) {
            if(decode.version === 0) {
                if(decode.data.length === 20) {
                    var extaddr = bitcoin.payments.p2wpkh({hash: decode.data, network: network_extras[i]}).address;
                    if(extaddr) {
                        await db.setAlias(extaddr, address);
                    }
                } else if(decode.data.length === 32) {
                    var extaddr = bitcoin.payments.p2wsh({hash: decode.data, network: network_extras[i]}).address;
                    if(extaddr) {
                        await db.setAlias(extaddr, address);
                    }
                }
            }
        }
    }
}

async function update_aliases() {
    console.log(network_extras_enabled);
    if(network_extras_enabled) {
        var addrs = await db.searchAddrs(network.bech32);
        for(var j in addrs) {
            await update_address_alias(addrs[j]);
        }
    }
}

async function migrate_after_sync() {
    await update_aliases();
}

;(async function() {
    var app = apiserver.start();
    apistream.start(app);

    try {
        height = await db.getLastBlockHeight() || 0;
        await txs_parser_log(height, 'start');
        await lastblock_rewrite();
        await block_check();
        apiserver.set_height(height);
        progress_enabled();
        await block_sync_tcp(true);
        await block_sync(true);
        await migrate_after_sync();
        progress_disabled();
        await txs_parser_log(height, aborting ? 'aborted' : 'synced');
        if(aborting) {
            await abort();
        }
    } catch(ex) {
        console.log('\r' + ex);
        await abort();
    }

    enable_block_writer_stream(true);
    mempool.cb_stream_unconf = function(unconf) {
        apistream.send_all(unconf);
    }

    async function worker() {
        try {
            await block_check();
            var new_block = await block_sync();
            mempool.update(new_block);

            if(aborting) {
                await abort();
            } else {
                setTimeout(worker, 1000);
            }
        } catch(ex) {
            console.log('\r' + ex);
            await abort();
        }
    }

    worker();
    apiserver.set_status(apiserver.status_code.SYNCED);


})();
