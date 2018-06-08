var opts = require('config');
var Rpc = require('./Rpc');
var bitcoin = require('bitcoinjs-lib');

var rpc = new Rpc(opts);
rpc.cb = function(cmd, err, res) {
    if(err) {
        return null;
    }
    return res;
}

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


;(async function() {
    var blockcount = await rpc.getBlockCount();
    console.log(blockcount);

    for(var i = blockcount - 9; i <= blockcount; i++) {
        console.log('--- block #' + i);
        var hash = await rpc.getBlockHash(i);
        var rawblock = await rpc.getBlock(hash, false);
        var block = bitcoin.Block.fromHex(rawblock);

        block.transactions.map(function(tx, i) {
            var txid = tx.getHash();
            var txid_str = txid.reverse().toString('hex');
            console.log('--- txid ' + txid_str);

            tx.ins.map(function(input) {
                var in_txid = input.hash;
                var n = input.index;

                if(n != 0xffffffff) {
                    console.log('in: ' + in_txid.reverse().toString('hex') + '-' + n);
                }
            });

            tx.outs.map(function(output, n) {
                var amount = output.value;
                var address = null;
                try {
                    address = bitcoin.address.fromOutputScript(output.script, network);
                } catch(ex) {}

                if(address) {
                    console.log('out: ' + txid_str + '-' + n + ' ' + amount + ' ' + address);
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
                        console.log('out: ' + txid_str + '-' + n + ' ' + amount + ' ' + addresses.join(','));
                    } else {
                        address = '#' + txid_str + '-' + n;
                        console.log('out: ' + txid_str + '-' + n + ' ' + amount + ' ' + address);
                    }
                }
            });
        });
    }
})();
