/*
Copyright (c) 2013-2014 Matteo Collina, http://matteocollina.com

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
*/
"use strict";

var mqtt = require("mqtt");
var mows = require("mows");
var http = require("http");
var https = require("https");
var async = require("async");
var fs    = require("fs");
var ascoltatori = require("ascoltatori");
var EventEmitter = require("events").EventEmitter;
var bunyan = require("bunyan");
var extend = require("extend");
var Client = require("./client");
var Stats = require("./stats");
var uuid = require("node-uuid");
var st = require("st");
var url = require("url");
var defaults = {
  port: 1883,
  backend: {
    json: false
  },
  stats: true,
  baseRetryTimeout: 1000,
  logger: {
    name: "mosca",
    level: 40,
    serializers: {
      client: clientSerializer,
      packet: packetSerializer
    }
  }
};
var nop = function() {};

/**
 * The Mosca Server is a very simple MQTT server that
 * provides a simple event-based API to craft your own MQTT logic
 * It supports QoS 0 & 1, without external storage.
 * It is backed by Ascoltatori, and it descends from
 * EventEmitter.
 *
 * Options:
 *  - `port`, the port where to create the server.
 *  - `backend`, all the options for creating the Ascoltatore
 *    that will power this server.
 *  - `ascoltatore`, the ascoltatore to use (instead of `backend`).
 *  - `baseRetryTimeout`, the retry timeout for the exponential
 *    backoff algorithm (default is 1s).
 *  - `logger`, the options for Bunyan.
 *  - `logger.childOf`, the parent Bunyan logger.
 *  - `persistence`, the options for the persistence.
 *     A sub-key `factory` is used to specify what persistence
 *     to use.
 *  - `secure`, an object that includes three properties:
 *     - `port`, the port that will be used to open the secure server
 *     - `keyPath`, the path to the key
 *     - `certPath`, the path to the certificate
 *  - `allowNonSecure`, starts both the secure and the unsecure sevrver.
 *  - `http`, an object that includes the properties:
 *     - `port`, the port that will be used to open the http server
 *     - `bundle`, serve the bundled mqtt client
 *     - `static`, serve a directory
 *  - `stats`, publish the stats every 10s (default false).
 *
 * Events:
 *  - `clientConnected`, when a client is connected;
 *    the client is passed as a parameter.
 *  - `clientDisconnecting`, when a client is being disconnected;
 *    the client is passed as a parameter.
 *  - `clientDisconnected`, when a client is disconnected;
 *    the client is passed as a parameter.
 *  - `published`, when a new message is published;
 *    the packet and the client are passed as parameters.
 *  - `subscribed`, when a client is subscribed to a topic;
 *    the topic and the client are passed as parameters.
 *  - `unsubscribed`, when a client is unsubscribed to a topic;
 *    the topic and the client are passed as parameters.
 *
 * @param {Object} opts The option object
 * @param {Function} callback The ready callback
 * @api public
 */
function Server(opts, callback) {

  if (!(this instanceof Server)) {
    return new Server(opts, callback);
  }

  EventEmitter.call(this);

  this.opts = extend(true, {}, defaults, opts);

  if (this.opts.persistence && this.opts.persistence.factory) {
    this.opts.persistence.factory(this.opts.persistence).wire(this);
  }

  callback = callback || function() {};

  this._dedupId = 0;
  this.clients = {};

  if (this.opts.logger.childOf) {
    this.logger = this.opts.logger.childOf;
    delete this.opts.logger.childOf;
    delete this.opts.logger.name;
    this.logger = this.logger.child(this.opts.logger);
  } else {
    this.logger = bunyan.createLogger(this.opts.logger);
  }

  if(this.opts.stats) {
    new Stats().wire(this);
  }

  var that = this;

  var serveWrap = function(connection) {
    // disable Nagle algorithm
    connection.stream.setNoDelay(true);
    new Client(connection, that);
  };

  // each Server has a dummy id for logging purposes
  this.id = this.opts.id || uuid().split("-")[0];

  this.ascoltatore = opts.ascoltatore || ascoltatori.build(this.opts.backend);
  this.ascoltatore.on("error", this.emit.bind(this));

  // initialize servers list
  this.servers = [];

  that.once("ready", function() {
    callback(null, that);
  });

  async.series([
    function(cb) {
      that.ascoltatore.on("ready", cb);
    },
    function(cb) {
      var server = null;
      var func = null;
      if (that.opts.http) {
        server = http.createServer(that.buildServe(that.opts.http));
        that.attachHttpServer(server);
        that.servers.push(server);
        that.opts.http.port = that.opts.http.port || 3000;
        server.listen(that.opts.http.port, cb);
      } else {
        cb();
      }
    },
    function(cb) {
      var server = null;
      if (that.opts.https) {
        server = https.createServer({
          key: fs.readFileSync(that.opts.secure.keyPath),
          cert: fs.readFileSync(that.opts.secure.certPath)
        }, that.buildServe(that.opts.https));
        that.attachHttpServer(server);
        that.servers.push(server);
        that.opts.https.port = that.opts.https.port || 3001;
        server.listen(that.opts.https.port, cb);
      } else {
        cb();
      }
    },
    function(cb) {
      var server = null;
      if (that.opts.secure && !that.opts.onlyHttp) {
        server = mqtt.createSecureServer(that.opts.secure.keyPath, that.opts.secure.certPath, serveWrap);
        that.servers.push(server);
        that.opts.secure.port = that.opts.secure.port || 8883;
        server.listen(that.opts.secure.port, cb);
      } else {
        cb();
      }
    }, function(cb) {
      if ((typeof that.opts.secure === 'undefined' || that.opts.allowNonSecure) && !that.opts.onlyHttp) {
        var server = mqtt.createServer(serveWrap);
        that.servers.push(server);
        server.listen(that.opts.port, cb);
      } else {
        cb();
      }
    }, function(cb) {

      var logInfo = {
        port: ((that.opts.onlyHttp || !that.opts.allowNonSecure) ? undefined : that.opts.port),
        securePort: (that.opts.secure || {}).port,
        httpPort: (that.opts.http || {}).port,
        httpsPort: (that.opts.https || {}).port
      };

      that.logger.info(logInfo, "server started");

      that.servers.forEach(function(server) {
        server.maxConnections = 100000;
      });
      that.emit("ready");
    }
  ]);

  that.on("clientConnected", function(client) {
    that.logger.info('client connected ', client.id);
    this.clients[client.id] = client;
    
    Object.keys(this.clients).forEach(function(c) {
      that.logger.info('Connected Clients :', c);  
    });
  });

  that.on("clientDisconnected", function(client) {
    that.logger.info('client discconnected ', client.id);
    delete this.clients[client.id];
    Object.keys(this.clients).forEach(function(c) {
      that.logger.info('Connected Clients ', c);  
    });
  });
}

module.exports = Server;

Server.prototype = Object.create(EventEmitter.prototype);

Server.prototype.toString = function() {
  return 'mosca.Server';
};

/**
 * Publishes a packet on the MQTT broker.
 *
 * @api public
 * @param {Object} packet The MQTT packet, it should include the
 *                        topic, payload, qos, and retain keys.
 * @param {Object} client The client object (internal)
 * @param {String} password The password
 */
Server.prototype.publish = function publish(packet, client, callback) {

  var that = this;
  var logger = this.logger;

  if (typeof client === 'function') {
    callback = client;
    client = null;
  } else if (client) {
    logger = client.logger;
  }

  if (!callback) {
    callback = nop;
  }

  var options = {
    qos: packet.qos,
    mosca: {
      client: client, // the client object
      packet: packet  // the packet being sent
    }
  };

  this.ascoltatore.publish(
    packet.topic,
    packet.payload,
    options,
    function() {
      that.storePacket(packet, function() {
        that.published(packet, client, function() {
          logger.debug({ packet: packet }, "published packet");
          that.emit("published", packet, client);
          callback();
        });
      });
    }
  );
};

/**
 * The function that will be used to authenticate users.
 * This default implementation authenticate everybody.
 * Override at will.
 *
 * @api public
 * @param {Object} client The MQTTConnection that is a client
 * @param {String} username The username
 * @param {String} password The password
 * @param {Function} callback The callback to return the verdict
 */
Server.prototype.authenticate = function(client, username, password, callback) {
  callback(null, true);
};

/**
 * The function that is called before after receiving a publish message but before
 * answering with puback for QoS 1 packet.
 * This default implementation does nothing
 * Override at will
 *
 * @api public
 * @param {Object} packet The MQTT packet
 * @param {Object} client The MQTTConnection that is a client
 * @param {Function} callback The callback to send the puback
 */
Server.prototype.published = function(packet, client, callback) {
  callback(null);
};

/**
 * The function that will be used to authorize clients to publish to topics.
 * This default implementation authorize everybody.
 * Override at will
 *
 * @api public
 * @param {Object} client The MQTTConnection that is a client
 * @param {String} topic The topic
 * @param {String} paylod The paylod
 * @param {Function} callback The callback to return the verdict
 */
Server.prototype.authorizePublish = function(client, topic, payload, callback) {
  callback(null, true);
};

/**
 * The function that will be used to authorize clients to subscribe to topics.
 * This default implementation authorize everybody.
 * Override at will
 *
 * @api public
 * @param {Object} client The MQTTConnection that is a client
 * @param {String} topic The topic
 * @param {Function} callback The callback to return the verdict
 */
Server.prototype.authorizeSubscribe = function(client, topic, callback) {
  callback(null, true);
};

/**
 * Store a packet for future usage, if needed.
 * Only packets with the retained flag are setted, or for which
 * there is an "offline" subscription".
 * This is a NOP, override at will.
 *
 * @api public
 * @param {Object} packet The MQTT packet to store
 * @param {Function} callback
 */
Server.prototype.storePacket = function(packet, callback) {
  if (callback) {
    callback();
  }
};

/**
 * Forward all the retained messages of the specified pattern to
 * the client.
 * This is a NOP, override at will.
 *
 * @api public
 * @param {String} pattern The topic pattern.
 * @param {MoscaClient} client The client to forward the packet's to.
 * @param {Function} callback
 */
Server.prototype.forwardRetained = function(pattern, client, callback) {
  if (callback) {
    callback();
  }
};

/**
 * Restores the previous subscriptions in the client
 * This is a NOP, override at will.
 *
 * @param {MoscaClient} client
 * @param {Function} callback
 */
Server.prototype.restoreClientSubscriptions = function(client, callback) {
  if (callback) {
    callback();
  }
};

/**
 * Forward all the offline messages the client has received when it was offline.
 * This is a NOP, override at will.
 *
 * @param {MoscaClient} client
 * @param {Function} callback
 */
Server.prototype.forwardOfflinePackets = function(client, callback) {
  if (callback) {
    callback();
  }
};

/**
 * Persist a client.
 * This is a NOP, override at will.
 *
 * @param {MoscaClient} client
 * @param {Function} callback
 */
Server.prototype.persistClient = function(client, callback) {
  if (callback) {
    callback();
  }
};

/**
 * Closes the server.
 *
 * @api public
 * @param {Function} callback The closed callback function
 */
Server.prototype.close = function(callback) {
  var that = this;
  var stuffToClose = [];
  Object.keys(that.clients).forEach(function(i) {
    stuffToClose.push(that.clients[i]);
  });

  that.servers.forEach(function(server) {
    stuffToClose.push(server);
  });

  async.each(stuffToClose, function(toClose, cb) {
    // console.log(toClose)
    // debugger;
    toClose.close(function(){
      cb();
    });
  }, function(err) {
    that.ascoltatore.close(function () {
      // debugger;
      that.logger.info("server closed");
      that.emit("closed");
      if (callback) {
        callback();
      }
    });
  });
};

/**
 * Attach a Mosca server to an existing http server
 *
 * @api public
 * @param {HttpServer} server
 */
Server.prototype.attachHttpServer = function(server) {
  var that = this;
  mows.attachServer(server, function(conn) {
    new Client(conn, that);
  });
};

/**
 * Create the serve logic for an http server.
 *
 * @api public
 * @param {Object} opts The same options of the constructor's
 *                      options, http or https.
 */
Server.prototype.buildServe = function(opts) {
  var mounts = [];

  if (opts.bundle) {
    mounts.push(st({
      path: __dirname + "/../public",
      url: "/",
      dot: false,
      index: false,
      passthrough: true
    }));
  }

  if (opts.static) {
    mounts.push(st({
      path: opts.static,
      dot: false,
      url: "/",
      passthrough: true
    }));
  }

  return function serve(req, res) {
    var cmounts = [].concat(mounts);

    function handle() {
      var mount = cmounts.shift();
      if(req.url =="/ping"){
        var d = new Date();
        res.end(d.toString()+","+d.getTime());
      }else if (mount) {
        mount(req, res, handle);
      } else {
        res.statusCode = 404;
        res.end("Not Found\n");
      }
    }

    handle();
  };
};

Server.prototype.nextDedupId = function() {
  return this._dedupId++;
};

/**
 * Serializises a client for Bunyan.
 *
 * @api private
 */
function clientSerializer(client) {
  return client.id;
}

/**
 * Serializises a packet for Bunyan.
 *
 * @api private
 */
function packetSerializer(packet) {
  var result = {};

  if (packet.messageId) {
    result.messageId = packet.messageId;
  }

  if (packet.topic) {
    result.topic = packet.topic;
  }

  if (packet.qos) {
    result.qos = packet.qos;
  }

  if (packet.unsubscriptions) {
    result.unsubscriptions = packet.unsubscriptions;
  }

  if (packet.subscriptions) {
    result.subscriptions = packet.subscriptions;
  }

  return result;
}
