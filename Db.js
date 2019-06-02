var rocksdb = require('level-packager')(require('rocksdb'));
var fs = require('fs-extra');
var UINT64 = require('cuint').UINT64;

var prefix = {
    rawblocks: 0x00,    // height, hash = rawblock, server_time
    blocks: 0x01,       // height = hash, time, sequence
    txs: 0x02,          // txid = height, time, sequence
    txouts: 0x03,       // txid, n = sequence, value, [address, ..]
    unspents: 0x04,     // address, sequence, txid, n = value
    addrvals: 0x05,     // address = value, utxo_count
    addrlogs: 0x06,     // address, sequence, type (0 - out | 1 - in) = txid, value
    markers: 0x07       // apikey = sequence, rollback
};

var rocksdb_opts = {
    keyEncoding: 'binary',
    valueEncoding: 'binary'
};

var uint8 = function(val) {
    var v = Buffer.alloc(1);
    v.writeUInt8(val);
    return v;
}

var uint32 = function(val) {
    var v = Buffer.alloc(4);
    v.writeUInt32BE(val);
    return v;
}

var uint64 = function(val) {
    var v = Buffer.alloc(8);
    if(val instanceof UINT64) {
        v.writeUInt16BE(val._a48, 0);
        v.writeUInt16BE(val._a32, 2);
        v.writeUInt16BE(val._a16, 4);
        v.writeUInt16BE(val._a00, 6);
    } else {
        v.writeUInt32BE(Math.floor(val / 0x100000000), 0);
        v.writeInt32BE(val & -1, 4);
    }
    return v;
}

var hex = function(val) {
    return Buffer.from(val, 'hex');
}

var str = function(val) {
    return Buffer.from(val, 'ascii');
}

var pad = function(len, val) {
    var v = Buffer.alloc(len);
    v.fill(val ? val : 0);
    return v;
}

var txid_min = pad(32, 0);
var txid_max = pad(32, 0xff);
var uint8_min = pad(1, 0);
var uint8_max = pad(1, 0xff);
var uint32_min = pad(4, 0);
var uint32_max = pad(4, 0xff);
var uint64_min = pad(8, 0);
var uint64_max = pad(8, 0xff);

function Db(opts) {
    var homepath = process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE;
    var blockpath = homepath + '/.blockstor/blocks';
    fs.mkdirsSync(blockpath);
    var db = rocksdb(blockpath, rocksdb_opts);
    var self = this;

    this.put = function(key, val) {
        return new Promise(function(resolve, reject) {
            db.put(key, val, function(err) {
                if(err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }

    this.get = function(key, cb) {
        return new Promise(function(resolve, reject) {
            db.get(key, function(err, res) {
                if(err) {
                    if(err.name == 'NotFoundError') {
                        resolve(null);
                    } else {
                        reject(err);
                    }
                    return;
                }
                if(cb) {
                    resolve(cb(res));
                } else {
                    resolve(res);
                }
            });
        });
    }

    this.del = function(key) {
        return new Promise(function(resolve, reject) {
            db.del(key, function(err) {
                if(err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }

    this.close = function() {
        return new Promise(function(resolve, reject) {
            db.close(function(err) {
                if(err) {
                    reject(err);
                    return;
                }
                resolve();
            });
        });
    }

    this.setRawBlock = function(height, hash, rawblock, server_time) {
        var key = Buffer.concat([
            uint8(prefix.rawblocks),
            uint32(height),
            hex(hash)
        ]);
        var val = Buffer.concat([
            Buffer.isBuffer(rawblock) ? rawblock : hex(rawblock),
            uint32(server_time)
        ]);
        return self.put(key, val);
    }

    this.getRawBlock = function(height, hash, binary) {
        var key = Buffer.concat([
            uint8(prefix.rawblocks),
            uint32(height),
            hex(hash)
        ]);
        if(binary) {
            return self.get(key, function(res) {
                return res.slice(0, -4);
            });
        } else {
            return self.get(key, function(res) {
                return res.slice(0, -4).toString('hex');
            });
        }
    }

    this.getRawBlockInfo = function(height, hash, binary) {
        var key = Buffer.concat([
            uint8(prefix.rawblocks),
            uint32(height),
            hex(hash)
        ]);
        if(binary) {
            return self.get(key, function(res) {
                return {rawblock: res.slice(0, -4), server_time: res.slice(-4).readUInt32BE(0)};
            });
        } else {
            return self.get(key, function(res) {
                return {rawblock: res.slice(0, -4).toString('hex'), server_time: res.slice(-4).readUInt32BE(0)};
            });
        }
    }

    this.delRawBlock = function(height, hash) {
        var key = Buffer.concat([
            uint8(prefix.rawblocks),
            uint32(height),
            hex(hash)
        ]);
        return self.del(key);
    }

    this.setBlockHash = function(height, hash, time, sequence) {
        var key = Buffer.concat([
            uint8(prefix.blocks),
            uint32(height)
        ]);
        var val = Buffer.concat([
            hex(hash),
            uint32(time),
            uint64(sequence)
        ]);
        return self.put(key, val);
    }

    this.getBlockHash = function(height) {
        var key = Buffer.concat([
            uint8(prefix.blocks),
            uint32(height)
        ]);
        return self.get(key, function(res) {
            return {
                hash: res.slice(0, 32).toString('hex'),
                time: res.readUInt32BE(32),
                sequence: res.readUInt32BE(36) * 0x100000000 + res.readUInt32BE(40)
            };
        });
    }

    this.delBlockHash = function(height) {
        var key = Buffer.concat([
            uint8(prefix.blocks),
            uint32(height)
        ]);
        return self.del(key);
    }

    this.setTx = function(txid, height, time, sequence) {
        var key = Buffer.concat([
            uint8(prefix.txs),
            hex(txid)
        ]);
        var val = Buffer.concat([
            uint32(height),
            uint32(time),
            uint64(sequence)
        ]);
        var ret = self.put(key, val);
        return ret;
    }

    this.getTx = function(txid) {
        var key = Buffer.concat([
            uint8(prefix.txs),
            hex(txid)
        ]);
        return self.get(key, function(res) {
            return {
                height: res.readUInt32BE(0),
                time: res.readUInt32BE(4),
                sequence: res.readUInt32BE(8) * 0x100000000 + res.readUInt32BE(12)
            };
        });
    }

    this.delTx = function(txid) {
        var key = Buffer.concat([
            uint8(prefix.txs),
            hex(txid)
        ]);
        var ret = self.del(key);
        return ret;
    }

    this.setTxout = function(txid, n, sequence, value, addresses) {
        var key = Buffer.concat([
            uint8(prefix.txouts),
            hex(txid),
            uint32(n)
        ]);
        var val = Buffer.concat([
            uint64(sequence),
            uint64(value),
            str(addresses.join(','))
        ]);
        return self.put(key, val);
    }

    this.getTxout = function(txid, n) {
        var key = Buffer.concat([
            uint8(prefix.txouts),
            hex(txid),
            uint32(n)
        ]);
        return self.get(key, function(res) {
            return {
                sequence: res.readUInt32BE(0) * 0x100000000 + res.readUInt32BE(4),
                value: UINT64(res.readUInt32BE(12), res.readUInt32BE(8)),
                addresses: res.slice(16).toString().split(',')
            };
        });
    }

    this.delTxout = function(txid, n) {
        var key = Buffer.concat([
            uint8(prefix.txouts),
            hex(txid),
            uint32(n)
        ]);
        return self.del(key);
    }

    this.setUnspent = function(address, sequence, txid, n, value) {
        var key = Buffer.concat([
            uint8(prefix.unspents),
            str(address),
            uint64(sequence),
            hex(txid),
            uint32(n)
        ]);
        var val = Buffer.concat([
            uint64(value)
        ]);
        return self.put(key, val);
    }

    this.getUnspent = function(address, sequence, txid, n) {
        var key = Buffer.concat([
            uint8(prefix.unspents),
            str(address),
            uint64(sequence),
            hex(txid),
            uint32(n)
        ]);
        return self.get(key, function(res) {
            return res.readUInt32BE(0);
        });
    }

    this.getUnspents = function(address, options) {
        return new Promise(function(resolve, reject) {
            var p_unspent = uint8(prefix.unspents);
            var str_addr = str(address);

            var db_options = {};
            if(options) {
                if(options.gte != null) {
                    db_options.gte = Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64(options.gte),
                        txid_min,
                        uint32_min
                    ]);
                } else if(options.gt != null) {
                    db_options.gt = Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64(options.gt),
                        txid_max,
                        uint32_max
                    ]);
                } else {
                    db_options.gte = Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64_min,
                        txid_min,
                        uint32_min
                    ]);
                }
                if(options.lte != null) {
                    db_options.lte = Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64(options.lte),
                        txid_max,
                        uint32_max
                    ]);
                } else if(options.lt != null) {
                    db_options.lt = Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64(options.lt),
                        txid_min,
                        uint32_min
                    ]);
                } else {
                    db_options.lte = Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64_max,
                        txid_max,
                        uint32_max
                    ]);
                }
                if(options.limit != null) {
                    db_options.limit = options.limit > 50000 ? 50000 : options.limit;
                } else {
                    db_options.limit = 1000;
                }
                if(options.seqbreak != 0) {
                    db_options.limit++;
                }
                if(options.reverse != null) {
                    db_options.reverse = Boolean(options.reverse);
                } else {
                    db_options.reverse = false;
                }
            } else {
                db_options = {
                    gte: Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64_min,
                        txid_min,
                        uint32_min
                    ]),
                    lte: Buffer.concat([
                        p_unspent,
                        str_addr,
                        uint64_max,
                        txid_max,
                        uint32_max
                    ])
                }
            }

            var unspents = [];
            db.createReadStream(db_options).on('data', function(res) {
                var len = res.key.length;
                unspents.push({
                    sequence: res.key.readUInt32BE(len - 44) * 0x100000000 + res.key.readUInt32BE(len - 40),
                    txid: res.key.slice(-36, -4).toString('hex'),
                    n: res.key.readUInt32BE(len - 4),
                    value: UINT64(res.value.readUInt32BE(4), res.value.readUInt32BE(0))
                });
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                if(options && options.seqbreak != 0) {
                    var last = unspents[unspents.length - 1];
                    if(last) {
                        var last_sequence = last.sequence;
                        do {
                            unspents.pop();
                            last = unspents[unspents.length - 1];
                        } while(last && last.sequence == last_sequence);
                        if(options.limit > 0 && unspents.length == 0) {
                            console.log('\rWARNING: getUnspents limit is too small');
                        }
                    }
                }
                resolve(unspents);
            });
        });
    }

    this.delUnspent = function(address, sequence, txid, n) {
        var key = Buffer.concat([
            uint8(prefix.unspents),
            str(address),
            uint64(sequence),
            hex(txid),
            uint32(n)
        ]);
        return self.del(key);
    }

    this.setAddrval = function(address, value, utxo_count) {
        var key = Buffer.concat([
            uint8(prefix.addrvals),
            str(address)
        ]);
        var val = Buffer.concat([
            uint64(value),
            uint32(utxo_count)
        ]);
        var ret = self.put(key, val);
        return ret;
    }

    this.getAddrval = function(address) {
        var key = Buffer.concat([
            uint8(prefix.addrvals),
            str(address)
        ]);
        return self.get(key, function(res) {
            return {
                value: UINT64(res.readUInt32BE(4), res.readUInt32BE(0)),
                utxo_count: res.readUInt32BE(8)
            };
        });
    }

    this.delAddrval = function(address) {
        var key = Buffer.concat([
            uint8(prefix.addrvals),
            str(address)
        ]);
        var ret = self.del(key);
        return ret;
    }

    this.setAddrlog = function(address, sequence, type, txid, value) {
        var key = Buffer.concat([
            uint8(prefix.addrlogs),
            str(address),
            uint64(sequence),
            uint8(type)
        ]);
        var val = Buffer.concat([
            hex(txid),
            uint64(value)
        ]);
        return self.put(key, val);
    }

    this.getAddrlog = function(address, sequence, type) {
        var key = Buffer.concat([
            uint8(prefix.addrlogs),
            str(address),
            uint64(sequence),
            uint8(type)
        ]);
        return self.get(key, function(res) {
            return {
                txid: res.slice(0, 32).toString('hex'),
                value: UINT64(res.readUInt32BE(36), res.readUInt32BE(32))
            };
        });
    }

    this.getAddrlogs = function(address, options) { // address, [options]
        return new Promise(function(resolve, reject) {
            var p_addrlog = uint8(prefix.addrlogs);
            var str_addr = str(address);

            var db_options = {};
            if(options) {
                if(options.gte != null) {
                    db_options.gte = Buffer.concat([
                        p_addrlog,
                        str_addr,
                        uint64(options.gte),
                        uint8_min
                    ]);
                } else if(options.gt != null) {
                    db_options.gt = Buffer.concat([
                        p_addrlog,
                        str_addr,
                        uint64(options.gt),
                        uint8_max
                    ]);
                } else {
                    db_options.gte = Buffer.concat([
                        p_addrlog,
                        str_addr,
                        uint64_min,
                        uint8_min
                    ]);
                }
                if(options.lte != null) {
                    db_options.lte = Buffer.concat([
                        p_addrlog,
                        str_addr,
                        uint64(options.lte),
                        uint8_max
                    ]);
                } else if(options.lt != null) {
                    db_options.lt = Buffer.concat([
                        p_addrlog,
                        str_addr,
                        uint64(options.lt),
                        uint8_min
                    ]);
                } else {
                    db_options.lte = Buffer.concat([
                        p_addrlog,
                        str_addr,
                        uint64_max,
                        uint8_max
                    ]);
                }
                if(options.limit != null) {
                    db_options.limit = options.limit > 50000 ? 50000 : options.limit;
                } else {
                    db_options.limit = 1000;
                }
                if(options.seqbreak != 0) {
                    db_options.limit++;
                }
                if(options.reverse != null) {
                    db_options.reverse = Boolean(options.reverse);
                } else {
                    db_options.reverse = true;
                }
            } else {
                db_options = {
                    gte: Buffer.concat([
                       p_addrlog,
                        str_addr,
                        uint64_min,
                        uint8_min
                    ]),
                    lte: Buffer.concat([
                        p_addrlog,
                        str_addr,
                        uint64_max,
                        uint8_max
                    ]),
                    reverse: true
                }
            }

            var addrlogs = [];
            db.createReadStream(db_options).on('data', function(res) {
                var len = res.key.length;
                addrlogs.push({
                    sequence: res.key.readUInt32BE(len - 9) * 0x100000000 + res.key.readUInt32BE(len - 5),
                    type: res.key.readUInt8(len - 1),
                    txid: res.value.slice(0, 32).toString('hex'),
                    value: UINT64(res.value.readUInt32BE(36), res.value.readUInt32BE(32))
                });
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                if(options && options.seqbreak != 0) {
                    var last = addrlogs[addrlogs.length - 1];
                    if(last) {
                        var last_sequence = last.sequence;
                        do {
                            addrlogs.pop();
                            last = addrlogs[addrlogs.length - 1];
                        } while(last && last.sequence == last_sequence);
                        if(options.limit > 0 && addrlogs.length == 0) {
                            console.log('\rWARNING: getAddrlogs limit is too small');
                        }
                    }
                }
                resolve(addrlogs);
            });
        });
    }

    this.checkAddrlogExist = function(address) {
        return new Promise(function(resolve, reject) {
            var p_addrlog = uint8(prefix.addrlogs);
            var str_addr = str(address);
            var start = Buffer.concat([
                p_addrlog,
                str_addr,
                uint64_min,
                uint8_min
            ]);
            var end = Buffer.concat([
                p_addrlog,
                str_addr,
                uint64_max,
                uint8_max
            ]);

            var exist = false;
            db.createReadStream({
                gte: start,
                lte: end,
                limit: 1
            }).on('data', function(res) {
                exist = true;
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                resolve(exist);
            });
        });
    }

    this.delAddrlog = function(address, sequence, type) {
        var key = Buffer.concat([
            uint8(prefix.addrlogs),
            str(address),
            uint64(sequence),
            uint8(type)
        ]);
        return self.del(key);
    }

    this.setMarker = function(apikey, sequence, rollback) {
        var key = Buffer.concat([
            uint8(prefix.markers),
            str(apikey)
        ]);
        var val = Buffer.concat([
            uint64(sequence),
            uint8(rollback)
        ]);
        return self.put(key, val);
    }

    this.getMarker = function(apikey) {
        var key = Buffer.concat([
            uint8(prefix.markers),
            str(apikey)
        ]);
        return self.get(key, function(res) {
            return {
                sequence: res.readUInt32BE(0) * 0x100000000 + res.readUInt32BE(4),
                rollback: res.readUInt8(8)
            };
        });
    }

    this.getMarkers = function() {
        return new Promise(function(resolve, reject) {
            var start = Buffer.concat([
                uint8(prefix.markers)
            ]);
            var end = Buffer.concat([
                uint8(prefix.markers + 1)
            ]);

            var apikeys = [];
            db.createReadStream({
                gte: start,
                lt: end
            }).on('data', function(res) {
                apikeys.push({
                    apikey: res.key.slice(1).toString(),
                    sequence: res.value.readUInt32BE(0) * 0x100000000 + res.value.readUInt32BE(4),
                    rollback: res.value.readUInt8(8)
                });
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                resolve(apikeys);
            });
        });
    }

    this.delMarker = function(apikey) {
        var key = Buffer.concat([
            uint8(prefix.markers),
            str(apikey)
        ]);
        return self.del(key);
    }

    this.getLastBlockHeight = function() {
        return new Promise(function(resolve, reject) {
            var start = Buffer.concat([
                uint8(prefix.blocks),
                uint32_min
            ]);
            var end = Buffer.concat([
                uint8(prefix.blocks),
                uint32_max
            ]);
            var last_height = null;
            db.createReadStream({
                gte: start,
                lte: end,
                reverse: true,
                limit: 1
            }).on('data', function(res) {
                last_height = res.key.readUInt32BE(1);
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                resolve(last_height);
            });
        });
    }

    this.count = function(prefix_id) {
        return new Promise(function(resolve, reject) {
            var start = Buffer.concat([
                uint8(prefix_id)
            ]);
            var end = Buffer.concat([
                uint8(prefix_id + 1)
            ]);

            var count = 0;
            db.createReadStream({
                gte: start,
                lt: end
            }).on('data', function(res) {
                count++;
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                resolve(count);
            });
        });
    }

    var search_limit = 20 + 1;
    this.searchAddresses = function(keyword) {
        return new Promise(function(resolve, reject) {
            var start = Buffer.concat([
                uint8(prefix.addrvals),
                str(keyword)
            ]);
            var end = Buffer.concat([
                uint8(prefix.addrvals),
                str(keyword.slice(0, -1) + String.fromCharCode(keyword.slice(-1).charCodeAt() + 1))
            ]);

            var addrs = [];
            db.createReadStream({
                gte: start,
                lt: end,
                limit: search_limit
            }).on('data', function(res) {
                addrs.push(res.key.slice(1).toString());
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                console.log(addrs);
                if(addrs.length >= search_limit) {
                    resolve(null);
                } else {
                    resolve(addrs);
                }
            });
        });
    }

    this.searchTxids = function(keyword) {
        return new Promise(function(resolve, reject) {
            var pad = keyword.length % 2 ? '0' : '';
            var start = Buffer.concat([
                uint8(prefix.txs),
                hex(keyword + pad)
            ]);
            var end = Buffer.concat([
                uint8(prefix.txs),
                hex(keyword.slice(0, -1) + String.fromCharCode(keyword.slice(-1).charCodeAt() + 1) + pad)
            ]);

            var txids = [];
            db.createReadStream({
                gte: start,
                lt: end,
                limit: search_limit
            }).on('data', function(res) {
                txids.push(res.key.slice(1).toString('hex'));
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                if(txids.length >= search_limit) {
                    resolve(null);
                } else {
                    resolve(txids);
                }
            });
        });
    }
}

module.exports = Db;
