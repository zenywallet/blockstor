function MemPool(opts, libs) {
    var bitcoin = libs.bitcoin;
    var rpc = libs.rpc;
    var db = libs.db;
    var network = libs.network;
    var self = this;

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
        var warning = rawmempool_addr_warning_cache[address];
        return {txouts: txouts, spents: spents, warning: warning};
    }

    this.cb_stream_unconf = function(unconf) {}

    function pushex(obj, key, value) {
        if(!obj[key]) {
            obj[key] = [value];
        } else {
            obj[key].push(value);
        }
    }

    function conv_uint64(uint64_val) {
        strval = uint64_val.toString();
        val = parseInt(strval);
        if(val > Number.MAX_SAFE_INTEGER) {
            return strval;
        }
        return val;
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

        var stream_addrs = {};
        var stream_addr_ins = {};
        var stream_addr_outs = {};
        var stream_addr_types = {};
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
                    if(stream_addr_outs[address]) {
                        stream_addr_outs[address].add(amount);
                    } else {
                        stream_addr_outs[address] = amount;
                    }
                } else {
                    var chunks;
                    chunks = bitcoin.script.decompile(output.script);
                    var find_addresses = [];
                    for(var k in chunks) {
                        var chunk = chunks[k];
                        if(Buffer.isBuffer(chunk) && chunk.length !== 1) {
                            address = null;
                            try {
                                address = bitcoin.payments.p2pkh({ pubkey: chunk, network: network }).address;
                            } catch(ex) {}

                            if(address) {
                                n_outs[address] = amount;
                                pushex(rawmempool_addr_txouts, address, txout_data);
                                find_addresses.push(address);
                            }
                        }
                    }
                    if(find_addresses.length > 1) {
                        for(var i in find_addresses) {
                            rawmempool_addr_warning[find_addresses[i]] = 1;
                            if(stream_addr_outs[find_addresses[i]]) {
                                stream_addr_outs[find_addresses[i]].add(amount);
                            } else {
                                stream_addr_outs[find_addresses[i]] = amount;
                            }
                            stream_addr_types[find_addresses[i]] = 2;
                        }
                    } else {
                        if(stream_addr_outs[address]) {
                            stream_addr_outs[address].add(amount);
                        } else {
                            stream_addr_outs[address] = amount;
                        }
                    }
                    if(!address) {
                        address = '@' + txid + '-' + n;
                        n_outs[address] = amount;
                        pushex(rawmempool_addr_txouts, address, txout_data);
                        if(stream_addr_outs[address]) {
                            stream_addr_outs[address].add(amount);
                        } else {
                            stream_addr_outs[address] = amount;
                        }
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
                            if(stream_addr_ins[address]) {
                                stream_addr_ins[address].add(txout.value);
                            } else {
                                stream_addr_ins[address] = txout.value;
                            }
                        }
                    } else {
                        var r_spent = rawmempool_spents[in_txid_n];
                        if(!r_spent) {
                            var txout_value = rawmempool_txouts[in_txid_n];
                            if(txout_value) {
                                for(var addr in txout_value) {
                                    pushex(rawmempool_addr_spents, addr, txout_value[addr]);
                                    if(stream_addr_ins[address]) {
                                        stream_addr_ins[address].add(txout_value[addr]);
                                    } else {
                                        stream_addr_ins[address] = txout_value[addr];
                                    }
                                }
                                rawmempool_spents[in_txid_n] = 1;
                                console.log('\rINFO: mempool spent=' + in_txid_n);
                            } else {
                                console.log('\rINFO: mempool vout not found ' + in_txid_n);
                            }
                        }
                    }
                }
            }));

            rawmempool_txs[txid] = 1;

            for(var addr in stream_addr_ins) {
                var conv_amount = conv_uint64(stream_addr_ins[addr]);
                if(stream_addrs[addr]) {
                    stream_addrs[addr][0] = conv_amount;
                } else {
                    var addrdata = {};
                    addrdata[0] = conv_amount;
                    stream_addrs[addr] = addrdata;
                }
            }
            for(var addr in stream_addr_outs) {
                var conv_amount = conv_uint64(stream_addr_outs[addr]);
                var type = stream_addr_types[addr] ? 2 : 1;
                if(stream_addrs[addr]) {
                    stream_addrs[addr][type] = conv_amount;
                } else {
                    var addrdata = {};
                    addrdata[type] = conv_amount;
                    stream_addrs[addr] = addrdata;
                }

            }
            self.cb_stream_unconf({unconf: txid, addrs: stream_addrs});
        }));

        rawmempool_addr_txouts_cache = Object.assign({}, rawmempool_addr_txouts);
        rawmempool_addr_spents_cache = Object.assign({}, rawmempool_addr_spents);
        rawmempool_addr_warning_cache = Object.assign({}, rawmempool_addr_warning);
        
        if(process.stdout.clearLine) {
            process.stdout.clearLine();
            process.stdout.write('\rmempool: count=' + Object.keys(rawmempool_txs).length);
        }

        update_flag = false;
    }
}

module.exports = MemPool;
