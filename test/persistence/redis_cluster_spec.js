"use strict";

var abstract = require("./abstract");
var RedisClusterPersistence= require("../../").persistence.RedisCluster;
var RedisCluster = require("redis-node-cluster").Cluster;

describe("mosca.persistence.RedisCluster", function() {

  var opts = {
    nodes:[
      {host:'localhost',port:7000},
      {host:'localhost',port:7001},
      {host:'localhost',port:7002},
      {host:'localhost',port:7003},
      {host:'localhost',port:7004},
      {host:'localhost',port:7005}
    ],
    settings:{
      redis:{
        retry_backoff:5
      }
    },
    ttl: {
      checkFrequency: 1000,
      subscriptions: 1000,
      packets: 1000
    }
  };
  
  abstract(RedisClusterPersistence, opts);

  afterEach(function(cb) {
    var flush = function() {
      var client = new RedisCluster(opts.nodes,opts.settings);
      client.flushdb(function() {
          client.disconnect(cb);
      });
    };

    if (this.secondInstance) {
      this.secondInstance.close(flush);
      this.secondInstance = null;
    } else {
      flush();
    }
  });

  describe("two clients", function() {

    it("should support restoring", function(done) {
      var client = { 
        id: "my client id - 42",
        clean: false,
        subscriptions: {
          "hello/#": {
            qos: 1
          }
        }
      };

      var packet = {
        topic: "hello/42",
        qos: 0,
        payload: new Buffer("world"),
        messageId: 42
      };

      var that = this;

      this.instance.storeSubscriptions(client, function() {
        that.instance.close(function() {
          that.instance = RedisClusterPersistence(opts, function(err, second) {
            second.storeOfflinePacket(packet, function() {
              second.streamOfflinePackets(client, function(err, p) {
                expect(p).to.eql(packet);
                done();
              });
            });
          });
        });
      });
    });

    it("should support synchronization", function(done) {
      var client = { 
        id: "my client id - 42",
        clean: false,
        subscriptions: {
          "hello/#": {
            qos: 1
          }
        }
      };

      var packet = {
        topic: "hello/42",
        qos: 0,
        payload: new Buffer("world"),
        messageId: 42
      };

      var that = this;
      that.secondInstance = RedisClusterPersistence(opts, function() {
        that.instance.storeSubscriptions(client, function() {
          setTimeout(function() {
            that.secondInstance.storeOfflinePacket(packet, function() {
              that.instance.streamOfflinePackets(client, function(err, p) {
                expect(p).to.eql(packet);
                done();
              });
            });
          }, 20);
        });
      });
    });
  });
});
