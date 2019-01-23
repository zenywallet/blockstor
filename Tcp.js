var net = require('net');
var bitcoin = require('bitcoinjs-lib');
var sha256 = bitcoin.crypto.sha256;

var debug = function(msg) {
    //console.log(msg);
}

var uint8 = function(val) {
    var v = Buffer.alloc(1);
    v.writeUInt8(val);
    return v;
}

var uint16 = function(val) {
    var v = Buffer.alloc(2);
    v.writeUInt16LE(val);
    return v;
}

var uint32 = function(val) {
    var v = Buffer.alloc(4);
    v.writeUInt32LE(val);
    return v;
}

var uint32be = function(val) {
    var v = Buffer.alloc(4);
    v.writeUInt32BE(val);
    return v;
}

var uint64 = function(val) {
    if(val > Number.MAX_SAFE_INTEGER) {
        throw 'Error: out of range';
    }

    var v = Buffer.alloc(8);
    v.writeInt32LE(val & -1, 0);
    v.writeUInt32LE(Math.floor(val / 0x100000000), 4);
    return v;
}

var hex = function(val) {
    return Buffer.from(val, 'hex');
}

var str = function(val) {
    return Buffer.from(val, 'ascii');
}

var pad = function(len) {
    var v = Buffer.alloc(len);
    v.fill(0);
    return v;
}

var var_int = function(val) {
    if (val < 0xfd) {
        return uint8(val);
    } else if (val <= 0xffff) {
        return Buffer.concat([uint8(0xfd), uint16(val)]);
    } else if (val <= 0xffffffff) {
        return Buffer.concat([uint8(0xfe), uint32(val)]);
    } else {
        return Buffer.concat([uint8(0xff), uint64(val)]);
    }
}

var var_str = function(val) {
    return Buffer.concat([var_int(val.length), str(val)]);
}

var command = function(val) {
    return Buffer.concat([str(val), pad(12 - val.length)]);
}

var inventory_type = {
    ERROR: 0,
    MSG_TX: 1,
    MSG_BLOCK: 2,
    MSG_FILTERED_BLOCK: 3,
    MSG_CMPCT_BLOCK: 4
};


function Reader(buf) {
    this.pos = 0;
    this.buf = buf;
}

Reader.prototype.var_int = function() {
    var val = this.buf.readUInt8(this.pos);
    this.pos++;
    if(val == 0xfd) {
        val = this.buf.readUInt16LE(this.pos);
        this.pos += 2;
    } else if(val == 0xfe) {
        val = this.buf.readUInt32LE(this.pos);
        this.pos += 4;
    } else if(val == 0xff) {
        val = this.buf.readUInt32LE(this.pos + 4) * 0x100000000 + this.buf.readUInt32LE(this.pos);
        this.pos += 8;

        if(val > Number.MAX_SAFE_INTEGER) {
            throw 'Error: out of range';
        }
    }
    return val;
}

Reader.prototype.uint8 = function() {
    var val = this.buf.readUInt8(this.pos);
    this.pos += 1;
    return val;
}

Reader.prototype.uint32 = function() {
    var val = this.buf.readUInt32LE(this.pos);
    this.pos += 4;
    return val;
}

Reader.prototype.hash = function() {
    var hash = this.buf.slice(this.pos, this.pos + 32);
    this.pos += 32;
    return hash;
}

Reader.prototype.var_str = function() {
    var length = this.var_int();
    var str = this.buf.slice(this.pos, this.pos + length);
    this.pos += length;
    return str.toString();
}


function Tcp(opts) {
    var self = this;

    var PROTOCOL_VERSION = opts.tcp.protocol_version;

    var msg_version = Buffer.concat([
        uint32(PROTOCOL_VERSION),
        uint64(0xd),
        uint64(Math.round(new Date().getTime() / 1000)),
        pad(26),
        pad(26),
        uint64(0xa5a5),
        var_str('/blockstor:0.1.0/'),
        uint32(0)
    ]);

    var message = function(cmd, payload) {
        var checksum = sha256(sha256(payload));
        return Buffer.concat([
            uint32be(opts.tcp.start_string),
            command(cmd),
            uint32(payload.length),
            checksum.slice(0, 4),
            payload
        ]);
    }

    var req_threshold = 50;
    var getdata_limit = 50;
    var getblock_check_wait = 10;
    var getblock_timeout = 5000;
    var getblock_retry_limit = Math.round(getblock_timeout / getblock_check_wait);

    var hashes = [];
    var req_hashes = [];
    var req_count = 0;
    var prev_hash = null;
    var height = 0;
    var block_queue = [];
    var aborting = false;
    var status = {};

    function send_message(data) {
        if(!aborting) {
            client.write(data);
        }
    }

    function send_getblocks() {
        send_message(message('getblocks', Buffer.concat([
            uint32(PROTOCOL_VERSION),
            var_int(1),
            prev_hash ? prev_hash : pad(32),
            pad(32)
        ])));
    }

    function check_getdata() {
        if(req_count <= req_threshold) {
            var getdata_count = 0;
            var hash = hashes.shift();
            if(hash) {
                var inventories = Buffer.alloc(0);
                do {
                    inventories = Buffer.concat([inventories, uint32(inventory_type.MSG_BLOCK), hash]);
                    req_hashes.push(hash);
                    req_count++;
                    getdata_count++;
                    if(getdata_limit && getdata_count >= getdata_limit) {
                        break;
                    }
                    hash = hashes.shift();
                } while(hash);
                send_message(message('getdata', Buffer.concat([var_int(getdata_count), inventories])));
            }
        }
    }

    function command_dispatch(header, body) {
        switch(header.command) {
        case 'version':
            status['version'] = 1;
            send_message(message('verack', Buffer.alloc(0)));
            break;

        case 'verack':
            debug('recv: varack');

            send_getblocks();
            break;

        case 'ping':
            send_message(message('pong', body));
            break;

        case 'pong':
            debug('recv: pong ' + body.toString('hex'));
            break;

        case 'inv':
            var reader = new Reader(body);
            var count = reader.var_int();
            debug('recv: inv count=' + count);

            for(var i = 0; i < count; i++) {
                var type = reader.uint32();
                var inv_hash = reader.hash();
                if(type == inventory_type.MSG_BLOCK) {
                    hashes.push(inv_hash);
                }
            }
            break;

        case 'block':
            var block = bitcoin.Block.fromBuffer(body);
            prev_hash = prev_hash || block.prevHash;

            var hash = req_hashes.shift();
            if(hash) {
                if(Buffer.compare(prev_hash, block.prevHash) == 0) {
                    height++;
                    block_queue.push({height: height, hash: hash, block: block, rawblock: body});
                    prev_hash = hash;
                } else {
                    debug('skip --- ' + Buffer.from(hash).reverse().toString('hex'));
                    req_count--;
                }
            } else {
                throw 'request hash not found';
            }

            if(req_hashes.length == 0 && hashes.length == 0) {
                send_getblocks();
            }
            break;

        case 'reject':
            var reader = new Reader(body);
            var reject_message = reader.var_str();
            var reject_ccode = reader.uint8();
            var reject_reason = reader.var_str();

            debug('recv: ' + header.command + ' message=' + reject_message + ' ccode=' + reject_ccode.toString(16) + ' reason=' + reject_reason);
            break;

        default:
            debug('recv: ' + header.command + ' - unhandled');
            break;
        }
    }

    var msg_data = null;
    function message_parser(data) {
        msg_data = msg_data ? Buffer.concat([msg_data, data]) : data;

        while(1) {
            if(msg_data.length < 24) {
                break;
            }
            var body_length = msg_data.readUInt32LE(16);
            var msg_length = 24 + body_length;
            if(msg_data.length < msg_length) {
                break;
            }
            var msg_header = {
                version: msg_data.readUInt32LE(0).toString(16),
                command: msg_data.slice(4, 16).toString().replace(/\0/g, ''),
                length: body_length,
                checksum: msg_data.readUInt32LE(20).toString(16)
            };
            var msg_body = msg_data.slice(24, msg_length);
            msg_data = msg_data.slice(msg_length);

            command_dispatch(msg_header, msg_body);
        }
    }

    var client;
    this.start = function(last_height, last_hash) {
        height = last_height || height;
        prev_hash = last_hash;
        client = new net.Socket();

        client.on('error', console.dir);

        client.connect(opts.tcp.port, opts.tcp.host, function() {
            debug('connect');
            status['connect'] = 1;

            send_message(message('version', msg_version));
        });

        client.on('data', function(data) {
            message_parser(data);
        });
    }

    this.stop = function() {
        aborting = true;
        client.end();
    }

    this.getblock = function() {
        return new Promise(function(resolve) {
            var retry_count = 0;
            function wait_block() {
                blockdata = block_queue.shift();
                if(blockdata) {
                    req_count--;
                    check_getdata();
                    return resolve(blockdata);
                } else {
                    check_getdata();
                    retry_count++;
                    if(retry_count > getblock_retry_limit) {
                        if(!status['connect']) {
                            console.log('ERROR: Cannot connect.');
                        } else if(status['connect'] && !status['version']) {
                            console.log('ERROR: No response.');
                        }
                        return resolve(null);
                    }
                }

                setTimeout(function() {
                    wait_block();
                }, getblock_check_wait);
            }

            wait_block();
        });
    }
}

module.exports = Tcp;
