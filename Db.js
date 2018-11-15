var rocksdb = require('level-packager')(require('rocksdb'));
var fs = require('fs-extra');
var UINT64 = require('cuint').UINT64;

var prefix = {
    blocks: 0x00,       // height = hash, time
    txouts: 0x01,       // txid, n = value, [address, ..]
    unspents: 0x02      // address, txid, n = value
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

var txid_min = hex('0000000000000000000000000000000000000000000000000000000000000000');
var txid_max = hex('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff');
var uint32_min = uint32(0);
var uint32_max = uint32(0xffffffff);

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

    this.setBlockHash = function(height, hash, time) {
        var key = Buffer.concat([
            uint8(prefix.blocks),
            uint32(height)
        ]);
        var val = Buffer.concat([
            hex(hash),
            uint32(time)
        ]);
        return self.put(key, val);
    }

    this.getBlockHash = function(height) {
        var key = Buffer.concat([
            uint8(prefix.blocks),
            uint32(height)
        ]);
        return self.get(key, function(res) {
            return {hash: res.slice(0, 32).toString('hex'), time: res.readUInt32BE(32)};
        });
    }

    this.delBlockHash = function(height) {
        var key = Buffer.concat([
            uint8(prefix.blocks),
            uint32(height)
        ]);
        return self.del(key);
    }

    this.setTxout = function(txid, n, value, addresses) {
        var key = Buffer.concat([
            uint8(prefix.txouts),
            hex(txid),
            uint32(n)
        ]);
        var val = Buffer.concat([
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
            return {value: UINT64(res.readUInt32BE(4), res.readUInt32BE(0)), addresses: res.slice(8).toString().split(',')};
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

    this.setUnspent = function(address, txid, n, value) {
        var key = Buffer.concat([
            uint8(prefix.unspents),
            str(address),
            hex(txid),
            uint32(n)
        ]);
        var val = Buffer.concat([
            uint64(value)
        ]);
        return self.put(key, val);
    }

    this.getUnspent = function(address, txid, n) {  // address, [txid, n]
        if(txid && n) {
            var key = Buffer.concat([
                uint8(prefix.unspents),
                str(address),
                hex(txid),
                uint32(n)
            ]);
            return self.get(key, function(res) {
                return res.readUInt32BE(0);
            });
        } else {
            var p_unspent = uint8(prefix.unspents);
            var str_addr = str(address);
            var start = Buffer.concat([
                p_unspent,
                str_addr,
                txid_min,
                uint32_min
            ]);
            var end = Buffer.concat([
                p_unspent,
                str_addr,
                txid_max,
                uint32_max
            ]);

            var unspents = [];
            return new Promise(function(resolve, reject) {
                db.createReadStream({
                    gte: start,
                    lte: end
                }).on('data', function(res) {
                    var len = res.key.length;
                    var key = res.key;
                    unspents.push({
                        txid: res.key.slice(len - 36, len - 4).toString('hex'),
                        n: res.key.slice(len - 4, len + 1).readUInt32BE(0),
                        value: UINT64(res.value.readUInt32BE(4), res.value.readUInt32BE(0))
                    });
                }).on('error', function(err) {
                    reject(err);
                }).on('close', function() {
                    reject(null);
                }).on('end', function() {
                    resolve(unspents);
                });
            });
        }
    }

    this.delUnspent = function(address, txid, n) {
        var key = Buffer.concat([
            uint8(prefix.unspents),
            str(address),
            hex(txid),
            uint32(n)
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
                last_height = res.key.slice(1).readUInt32BE(0);
            }).on('error', function(err) {
                reject(err);
            }).on('close', function() {
                reject(null);
            }).on('end', function() {
                resolve(last_height);
            });
        });
    }
}

module.exports = Db;
