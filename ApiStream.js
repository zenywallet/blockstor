var WebSocket = require('ws');

function ApiStream(opts) {
    var wss;
    var http_server;
    var self = this;

    function debug(msg) {
        if(process.stdout.clearLine) {
            process.stdout.clearLine();
            console.log("\r" + msg);
        } else {
            console.log(msg);
        }
    }

    this.send_all = function(data) {
        wss.clients.forEach(function(ws) {
            if(ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(data));
            }
        });
    }

    this.start = function(app) {
        if(app) {
            http_server = require('http').createServer(app);
            wss = new WebSocket.Server({server: http_server});
        } else {
            wss = new WebSocket.Server({port: opts.server.ws_port});
        }

        function get_client_count() {
            var client_count = 0;
            wss.clients.forEach(function(ws) {
                client_count++;
            });
            return client_count;
        }

        function heartbeat() {
            this.isAlive = true;
        }

        function noop() {}

        wss.on('connection', function connection(ws, req) {
            ws.subscribe = {};
            ws.isAlive = true;
            ws.ip = (req.headers['x-forwarded-for'] || '').split(/\s*,\s*/)[0] || req.connection.remoteAddress;
            debug('ws connect ip=' + ws.ip + ' count=' + get_client_count());

            ws.on('pong', heartbeat);

            ws.on('close', function() {
                debug('ws close ip=' + ws.ip + ' count=' + get_client_count());
            });

            ws.on('message', function(message) {
                debug(message);
                try {
                    var data = JSON.parse(message);
                } catch(e) {
                    debug(e.name + ': ' + e.message);
                }
            });
        });

        var heartbeat_intval = setInterval(function() {
            wss.clients.forEach(function(ws) {
                if(ws.isAlive === false) {
                    return ws.terminate();
                }
                ws.isAlive = false;
                ws.ping(noop);
            });
        }, 10000);

        if(app) {
            http_server.listen(opts.server.http_port);
        }

        ;(function test() {
            self.send_all({txs: 'hello!'});
            setTimeout(test, 5000);
        })();
    }
}

module.exports = ApiStream;
