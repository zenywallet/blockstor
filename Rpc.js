var bitcoin = require('bitcoin');
var commands = require('./commands');

function Rpc(opts) {
    var client = new bitcoin.Client(opts.rpc);
    var self = this;

    for(var cmd in commands) {
        (function(cmd) {
            self[cmd] = function() {
                var args = Array.prototype.slice.call(arguments);
                return new Promise(function(resolve) {
                    args.push(function(err, res) {
                        resolve(self.cb(cmd, err, res));
                    });
                    client[cmd].apply(client, args);
                });
            }
        })(cmd);
    }
}

Rpc.prototype.cb = function(cmd, err, res) {
    return {err: err, res: res};
}

module.exports = Rpc;
