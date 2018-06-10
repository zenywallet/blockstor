var opts = require('config');
var Rpc = require('./Rpc');
var bitcoin = require('bitcoinjs-lib');
var Db = require('./Db');
var rpc = new Rpc(opts);
rpc.cb = function(cmd, err, res) {
    if(err) {
        return null;
    }
    return res;
}
var db = new Db(opts);

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


;(async function() {
    var blockcount = await rpc.getBlockCount();
    console.log(blockcount);

    var last_height = await db.getLastBlockHeight() || 0;
    console.log(last_height);

    for(var height = last_height; height <= blockcount; height++) {
        var hash = await rpc.getBlockHash(height);
        var rawblock = await rpc.getBlock(hash, false);
        var block = bitcoin.Block.fromHex(rawblock);
        var time = block.timestamp;

        await db.setBlockHash(height, hash, time);

        await txs_parser(block,
            async function txs(txid) {
            },
            async function txins(txid, n, txout) {
                for(var i in txout.addresses) {
                    var address = txout.addresses[i];
                    await db.delUnspent(address, txid, n);
                }
            },
            async function txouts(txid, n, amount, addresses) {
                await db.setTxout(txid, n, amount, addresses);

                for(var i in addresses) {
                    var address = addresses[i];
                    await db.setUnspent(address, txid, n, amount);
                }
            }
        );

        if(height % 100 == 0) {
            console.log(height);
        }
    }

    //console.log(await db.getUnspent('some address here!'));
})();
