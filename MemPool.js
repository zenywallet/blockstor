function MemPool(opts, libs) {
    var bitcoin = libs.bitcoin;
    var rpc = libs.rpc;
    var db = libs.db;
    var network = libs.network;

    var rawmempool_rawtxs = {};
    var rawmempool_rawtxobjs = {};
    var rawmempool_txs = {};
    var rawmempool_spents = {};
    var rawmempool_txouts = {};
    var rawmempool_addr_txouts = {};
    var rawmempool_addr_spents = {};
    var rawmempool_addr_warning = {};

    var rawmempool_addr_txouts_cache = {};
    var rawmempool_addr_spents_cache = {};
    var rawmempool_addr_warning_cache = {};

    this.unconfs = function(address) {
        var txouts = rawmempool_addr_txouts_cache[address];
        var spents = rawmempool_addr_spents_cache[address];
        return {txouts: txouts, spents: spents};
    }

    function pushex(obj, key, value) {
        if(!obj[key]) {
            obj[key] = [value];
        } else {
            obj[key].push(value);
        }
    }

    var update_flag = false;
    var reset_flag = false;
    this.update = async function(reset) {
        if(update_flag) {
            if(reset) {
                reset_flag = true;
            }
            return;
        }
        update_flag = true;

        var mempool = await rpc.getRawMemPool();

        if(reset || reset_flag) {
            rest_flag = false;

            rawmempool_rawtxs_tmp = {};
            for(var i in mempool) {
                var txid = mempool[i];
                if(rawmempool_rawtxs[txid]) {
                    rawmempool_rawtxs_tmp[txid] = rawmempool_rawtxs[txid];
                }
            }
            rawmempool_rawtxs = rawmempool_rawtxs_tmp;
            rawmempool_rawtxobjs_tmp = {};
            for(var i in mempool) {
                var txid = mempool[i];
                if(rawmempool_rawtxobjs[txid]) {
                    rawmempool_rawtxobjs_tmp[txid] = rawmempool_rawtxobjs[txid];
                }
            }
            rawmempool_rawtxobjs = rawmempool_rawtxobjs_tmp;
            rawmempool_txs = {};
            rawmempool_txouts = {};
            rawmempool_spents = {};
            rawmempool_addr_txouts = {};
            rawmempool_addr_spents = {};
            rawmempool_addr_warning = {};
        }

        for(var i in mempool) {
            var txid = mempool[i];
            if(rawmempool_txs[txid]) {
                continue;
            }

            var rawtx = rawmempool_rawtxs[txid];
            if(!rawtx) {
                var rawtx = await rpc.getRawTransaction(txid);  // Do not call multiple
                if(!rawtx) {
                    continue;
                }
                rawmempool_rawtxs[txid] = rawtx;
            }
        }

        await Promise.all(mempool.map(async function(txid) {
            if(rawmempool_txs[txid]) {
                return;
            }

            var tx = rawmempool_rawtxobjs[txid];
            if(!tx) {
                var rawtx = rawmempool_rawtxs[txid];
                if(!rawtx) {
                    return;
                }
                tx = new bitcoin.Transaction.fromHex(rawtx);
                rawmempool_rawtxobjs[txid] = tx;
            }

            tx.outs.map(function(output, n) {
                var amount = output.value;
                var out_txid_n = txid + '-' + n;
                var txout_data = {txid: txid, n: n, value: amount};

                var n_outs = {};
                var address = null;
                try {
                    address = bitcoin.address.fromOutputScript(output.script, network);
                } catch(ex) {}

                if(address) {
                    n_outs[address] = amount;
                    pushex(rawmempool_addr_txouts, address, txout_data);
                } else {
                    var chunks;
                    chunks = bitcoin.script.decompile(output.script);
                    var find_count = 0;
                    for(var k in chunks) {
                        var chunk = chunks[k];
                        if(Buffer.isBuffer(chunk) && chunk.length !== 1) {
                            address = null;
                            try {
                                address = bitcoin.ECPair.fromPublicKeyBuffer(chunk, network).getAddress();
                            } catch(ex) {}

                            if(address) {
                                n_outs[address] = amount;
                                pushex(rawmempool_addr_txouts, address, txout_data);
                                find_count++;
                            }
                        }
                    }
                    if(find_count > 1) {
                        rawmempool_addr_warning[address] = 1;
                    }
                    if(!address) {
                        address = '#' + txid + '-' + n;
                        n_outs[address] = amount;
                        pushex(rawmempool_addr_txouts, address, txout_data);

                        var asm = bitcoin.script.toASM(chunks);
                        console.log("\rINFO: Unknown address asm=" + asm);
                    }
                }
                rawmempool_txouts[out_txid_n] = n_outs;
            });

            await Promise.all(tx.ins.map(async function(input) {
                var in_txid = Buffer.from(input.hash).reverse().toString('hex');
                var n = input.index;
                var in_txid_n = in_txid + '-' + n;
                if(n != 0xffffffff) {
                    var txout = await db.getTxout(in_txid, n);
                    if(txout) {
                        rawmempool_spents[in_txid_n] = 1;
                        for(var i in txout.addresses) {
                            var address = txout.addresses[i];
                            pushex(rawmempool_addr_spents, address, {txid: in_txid, n: n, value: txout.value});
                        }
                    } else {
                        var r_spent = rawmempool_spents[in_txid_n];
                        if(!r_spent) {
                            var txout_value = rawmempool_txouts[in_txid_n];
                            if(txout_value) {
                                for(var addr in txout_value) {
                                    pushex(rawmempool_addr_spents, addr, txout_value[addr]);
                                }
                                rawmempool_spents[in_txid_n] = 1;
                                console.log("\rINFO: mempool spent=" + in_txid_n);
                            } else {
                                console.log("\rINFO: mempool vout not found " + in_txid_n);
                            }
                        }
                    }
                }
            }));

            rawmempool_txs[txid] = 1;
        }));
        rawmempool_addr_txouts_cache = Object.assign({}, rawmempool_addr_txouts);
        rawmempool_addr_spents_cache = Object.assign({}, rawmempool_addr_spents);
        rawmempool_addr_warning_cache = Object.assign({}, rawmempool_addr_warning);
        
        if(process.stdout.clearLine) {
            process.stdout.clearLine();
            process.stdout.write("\rmempool: count=" + Object.keys(rawmempool_txs).length);
        }

        update_flag = false;
    }
}

module.exports = MemPool;
