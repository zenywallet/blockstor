var WebSocket = require('ws');

function WebSockStream(opts) {
    var ws;
    var _open = function() {};
    var _receive = function() {};
    var _active = true;
    var queue = [];
    var reconnect_timer;
    var ping_timer;
    var self = this;

    function heartbeat() {
        clearTimeout(ping_timer);
        ping_timer = setTimeout(function() {
            ws.terminate();
        }, 30000);
    }

    this.connected = function() {
        return (ws.readyState === WebSocket.OPEN);
    }

    function connect() {
        ws = new WebSocket(opts.ws_url);

        ws.on('error', function(err) {
            console.log(err);
        });

        ws.on('open', function() {
            heartbeat();
            if(self.connected()) {
                _open();
                while(queue.length > 0) {
                    ws.send(queue.shift());
                }
            }
        });

        ws.on('ping', heartbeat);

        ws.on('close', function() {
            console.log('close');
            if(_active) {
                reconnect_timer = setTimeout(function() {
                    if(_active) {
                        connect();
                    }
                }, 10000);
            }
        });

        ws.on('message', function(msg) {
            try {
                _receive(JSON.parse(msg));
            } catch(e) {
                console.log(e.name + ': ' + e.message);
            }
        });
    }

    this.start = function() {
        connect();
    }

    this.open = function(callback) {
        _open = callback;
    }

    this.receive = function(callback) {
        _receive = callback;
    }

    this.send = function(data) {
        var json_str = JSON.stringify(data);
        if(self.connected()) {
            while(queue.length > 0) {
                ws.send(queue.shift());
            }
            ws.send(json_str);
        } else {
            queue.push(json_str);
        }
    }

    this.disconnect = function() {
        _active = false;
        clearTimeout(reconnect_timer);
        clearTimeout(ping_timer);
        ws.close();
    }
}

var wsstream = new WebSockStream({ws_url: 'ws://localhost:8000/api'});

wsstream.open(function() {
    console.log('connected');
});

wsstream.receive(function(data) {
    console.log(JSON.stringify(data));
});

wsstream.start();

//;(function test() {
//    wsstream.send({cmd: 'from client'});
//    test_timer = setTimeout(test, 5000);
//})();
