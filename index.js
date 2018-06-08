var opts = require('config');
var Rpc = require('./Rpc');

var rpc = new Rpc(opts);
rpc.cb = function(cmd, err, res) {
    if(err) {
        return null;
    }
    return res;
}

;(async function() {
    var blockchaininfo = await rpc.getBlockchainInfo();
    console.log(blockchaininfo);

    var blockcount = await rpc.getBlockCount();
    console.log(blockcount);

    for(var i = 0; i < 10; i++) {
        var hash = await rpc.getBlockHash(i);
        var rawblock = await rpc.getBlock(hash, false);
        console.log(rawblock);
    }
})();
