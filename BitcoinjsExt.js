function BitcoinjsExt(bitcoinjs, opts) {
    for(var key in opts.networks) {
        bitcoinjs.networks[key] = opts.networks[key];
    }

    return bitcoinjs;
}

module.exports = BitcoinjsExt;
