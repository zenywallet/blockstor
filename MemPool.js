function MemPool(opts, libs) {
    var bitcoin = libs.bitcoin;
    var rpc = libs.rpc;
    var db = libs.db;
    var network = libs.network;
    var self = this;

    var mp_txobjs = {};
    var mp_txouts = {};
    var mp_txins = {};
    var mp_spents = {};
    var mp_txaddrs = {};

    var mp_addr_txouts = {};
    var mp_addr_spents = {};
    var mp_addr_warning = {};

    var mp_txobjs_cache = {};
    var mp_txouts_cache = {};
    var mp_txins_cache = {};
    var mp_txaddrs_cache = {};

    var mp_addr_txouts_cache = {};
    var mp_addr_spents_cache = {};
    var mp_addr_warning_cache = {};

    this.unconfs = function(address) {
        var txouts = mp_addr_txouts_cache[address];
        var spents = mp_addr_spents_cache[address];
        var warning = mp_addr_warning_cache[address];
        return {txouts: txouts, spents: spents, warning: warning};
    }

    this.cb_stream_unconf = function(unconf) {}

    var mp_tx_unconfs = null;
    this.stream_unconfs = function() {
        var tx_unconfs = [];
        if(!mp_tx_unconfs) {
            for(var txid in mp_txaddrs_cache) {
                tx_unconfs.push({txid: txid, addrs: mp_txaddrs_cache[txid]});
            }
            mp_tx_unconfs = tx_unconfs;
        }
        return mp_tx_unconfs || tx_unconfs;
    }

    function pushex(obj, key, value) {
        if(!obj[key]) {
            obj[key] = [value];
        } else {
            obj[key].push(value);
        }
    }

    function addex(obj, key, value) {
        if(!obj[key]) {
            obj[key] = value;
        } else {
            obj[key].add(value);
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

    var updating = false;
    var reset_reserve = false;
    this.update = async function(reset) {
        reset_reserve = reset_reserve || reset;
        if(updating) {
            return;
        }
        updating = true;

        var mp_txids = await rpc.getRawMemPool();
        var mp_new_txids = [];

        if(reset_reserve) {
            reset_reserve = false;

            mp_txobjs = {};
            mp_txouts = {};
            mp_txins = {};
            mp_spents = {};
            mp_txaddrs = {};

            mp_addr_txouts = {};
            mp_addr_spents = {};
            mp_addr_warning = {};

            for(var i in mp_txids) {
                var txid = mp_txids[i];
                if(mp_txobjs_cache[txid]) {
                    mp_txobjs[txid] = mp_txobjs_cache[txid];
                    mp_txouts[txid] = mp_txouts_cache[txid];
                    mp_txins[txid] = mp_txins_cache[txid];
                    mp_txaddrs[txid] = mp_txaddrs_cache[txid];

                    var txins = mp_txins[txid];
                    for(var j in txins) {
                        var txin = txins[j];
                        var in_txid_n = txin.txid + '-' + txin.n;
                        mp_spents[in_txid_n] = 1;
                    }
                    var txouts = mp_txouts[txid];
                    for(var j in txouts) {
                        var txout = txouts[j];
                        var warning = txout.addresses.length > 1;
                        for(var k in txout.addresses) {
                            var addr = txout.addresses[k];
                            pushex(mp_addr_txouts, addr, {txid: txout.txid, n: txout.n, value: txout.value});
                            if(warning) {
                                mp_addr_warning[addr] = 1;
                            }
                        }
                    }
                    var spents = mp_txins[txid];
                    for(var j in spents) {
                        var spent = spents[j];
                        for(var k in spent.addresses) {
                            var addr = spent.addresses[k];
                            pushex(mp_addr_spents, addr, {txid: spent.txid, n: spent.n, value: spent.value});
                        }
                    }
                } else {
                    mp_new_txids.push(txid);
                }
            }
        } else {
            for(var i in mp_txids) {
                var txid = mp_txids[i];
                if(!mp_txobjs_cache[txid]) {
                    mp_new_txids.push(txid);
                }
            }
        }

        for(var i in mp_new_txids) {
            var txid = mp_new_txids[i];
            var rawtx = await rpc.getRawTransaction(txid);
            if(rawtx) {
                mp_txobjs[txid] = new bitcoin.Transaction.fromHex(rawtx);
            }
        }

        for(var i in mp_new_txids) {
            var txid = mp_new_txids[i];
            var tx = mp_txobjs[txid];
            for(var n in tx.outs) {
                var output = tx.outs[n];
                var addresses = get_script_addresses(output.script, network);
                if(addresses == null) {
                    addresses = ['@' + txid + '-' + n];
                }
                pushex(mp_txouts, txid, {n: n, value: output.value, addresses: addresses});

                var warning = addresses.length > 1;
                for(var i in addresses) {
                    var addr = addresses[i];
                    pushex(mp_addr_txouts, addr, {txid: txid, n: n, value: output.value});
                    if(warning) {
                        mp_addr_warning[addr] = 1;
                    }
                }
            };
        };

        for(var i in mp_new_txids) {
            var txid = mp_new_txids[i];
            var tx = mp_txobjs[txid];
            for(var j in tx.ins) {
                var input = tx.ins[j];
                var in_txid = Buffer.from(input.hash).reverse().toString('hex');
                var n = input.index;
                if(n != 0xffffffff) {
                    var txout = await db.getTxout(in_txid, n);
                    if(txout) {
                        pushex(mp_txins, txid, {txid: in_txid, n: n, value: txout.value, addresses: txout.addresses});
                        var in_txid_n = in_txid + '-' + n;
                        if(mp_spents[in_txid_n]) {
                            console.log('\rINFO: mempool double spending ' + in_txid_n);
                        }
                        mp_spents[in_txid_n] = 1;

                        for(var i in txout.addresses) {
                            var addr = txout.addresses[i];
                            pushex(mp_addr_spents, addr, {txid: in_txid, n: n, value: txout.value});
                        }
                    } else {
                        var find_txout = false;
                        var txouts = mp_txouts[in_txid];
                        for(var i in txouts) {
                            var txout = txouts[i];
                            if(txout.n == n) {
                                find_txout = true;
                                pushex(mp_txins, txid, {txid: in_txid, n: n, value: txout.value, addresses: txout.addresses});
                                var in_txid_n = in_txid + '-' + n;
                                if(mp_spents[in_txid_n]) {
                                    console.log('\rINFO: mempool double spending ' + in_txid_n);
                                } else {
                                    console.log('\rINFO: mempool spent ' + in_txid_n);
                                }
                                mp_spents[in_txid_n] = 1;

                                for(var i in txout.addresses) {
                                    var addr = txout.addresses[i];
                                    pushex(mp_addr_spents, addr, {txid: in_txid, n: n, value: txout.value});
                                }
                                break;
                            }
                        }
                        if(!find_txout) {
                            console.log('\INFO: mempool vout not found ' + in_txid_n);
                        }
                    }
                }
            };
        };

        mp_addr_txouts_cache = Object.assign({}, mp_addr_txouts);
        mp_addr_spents_cache = Object.assign({}, mp_addr_spents);
        mp_addr_warning_cache = Object.assign({}, mp_addr_warning);

        mp_txouts_cache = Object.assign({}, mp_txouts);
        mp_txins_cache = Object.assign({}, mp_txins);
        mp_txobjs_cache = Object.assign({}, mp_txobjs);

        var txaddrs = [];
        for(var i in mp_new_txids) {
            var stream_addrs = {};
            var stream_addr_ins = {};
            var stream_addr_outs = {};
            var stream_addr_types = {};

            var txid = mp_new_txids[i];
            var txins = mp_txins_cache[txid];
            var txouts = mp_txouts_cache[txid];
            for(var j in txins) {
                var txin = txins[j];
                for(var k in txin.addresses) {
                    var addr = txin.addresses[k];
                    addex(stream_addr_ins, addr, txin.value);
                }
            }

            for(var j in txouts) {
                var txout = txouts[j];
                var warning = txout.addresses.length > 1;
                for(var k in txout.addresses) {
                    var addr = txout.addresses[k];
                    addex(stream_addr_outs, addr, txout.value);
                    if(warning) {
                        stream_addr_types[addr] = 2;
                    }
                }
            }

            for(var addr in stream_addr_ins) {
                var conv_value = conv_uint64(stream_addr_ins[addr]);
                if(stream_addrs[addr]) {
                    stream_addrs[addr][0] = conv_value;
                } else {
                    var addrdata = {};
                    addrdata[0] = conv_value;
                    stream_addrs[addr] = addrdata;
                }
            }
            for(var addr in stream_addr_outs) {
                var conv_value = conv_uint64(stream_addr_outs[addr]);
                var type = stream_addr_types[addr] ? 2 : 1;
                if(stream_addrs[addr]) {
                    stream_addrs[addr][type] = conv_value;
                } else {
                    var addrdata = {};
                    addrdata[type] = conv_value;
                    stream_addrs[addr] = addrdata;
                }
            }

            txaddrs.push({txid: txid, addrs: stream_addrs});
        }
        if(txaddrs.length > 0) {
            self.cb_stream_unconf({mempool: txaddrs});
            for(var i in txaddrs) {
                var txaddr = txaddrs[i];
                mp_txaddrs[txaddr.txid] = txaddr.addrs;
            }
        }
        mp_txaddrs_cache = Object.assign({}, mp_txaddrs);
        mp_tx_unconfs = null;

        if(process.stdout.clearLine) {
            process.stdout.clearLine();
            process.stdout.write('\rmempool: count=' + Object.keys(mp_txids).length);
        }

        updating = false;
    }
}

module.exports = MemPool;
