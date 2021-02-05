/**
 * Cryptonote Node.JS Pool
 * https://github.com/dvandal/cryptonote-nodejs-pool
 *
 * Payments processor
 **/

// Load required modules
var fs = require('fs');
var async = require('async');

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet, config.api);
var notifications = require('./notifications.js');
var utils = require('./utils.js');

// Initialize log system
var logSystem = 'payments';
require('./exceptionWriter.js')(logSystem);

/**
 * Run payments processor
 **/

log('info', logSystem, 'Started');

if (!config.poolServer.paymentId) config.poolServer.paymentId = {};
if (!config.poolServer.paymentId.addressSeparator) config.poolServer.paymentId.addressSeparator = "+";
if (!config.payments.priority) config.payments.priority = 0;


var lock = false;//paranoid check...
var cmd = 0;

function Pay(transferCommands, callback) {

  var transferCmd = [];
  var notify_miners = [];

  log('info', logSystem, 'function pay running... transferCommands: %d, cmd: %d',[transferCommands.length, cmd]);

  if(transferCommands[cmd]){
    transferCmd = transferCommands[cmd];
  }else{
    cmd = 0;
    log('info', logSystem, 'function pay stopping...');
		return callback(false,null);
  }
  cmd++;
  async.waterfall([
      function (cback) {
        createTransaction(transferCmd, function (error, result) {
          if (error || !result) {
            cback(true,result);
            return;
          }
          cback(error, result);
        })
      },
      function (command, cback) {
          sendTransaction(command, function (error, result) {
              if (error || !result) {
                cback(true,result);
                return;
              }
              cback(error, result)
          })
      },
      function (command, cback) {
        if (!command.sent) {
          cback(true);
          return;
        }
      //cmd is used to store data in redis database. See -> ZRANGE coinname:payments:addres 0 -1
        var now = cmd + Date.now() / 1000 | 0;

        command.redis.push(['zadd', config.coin + ':payments:all', now, [
                command.hash,
                command.amount,
                command.fee,
                command.mixin,
                Object.keys(command.rpc.transaction.transfers).length
        ].join(':')]);

        var notify_miners_on_success = [];

        for (var i = 0; i < command.rpc.transaction.transfers.length; i++) {
          var destination = command.rpc.transaction.transfers[i];

          if (command.rpc.transaction.payment_id){
            destination.address += config.poolServer.paymentId.addressSeparator + command.rpc.transaction.payment_id;
          }

          command.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                command.hash,
                destination.amount,
                command.fee,
                command.mixin
                ].join(':')]);

          notify_miners_on_success.push(destination);
        }//for

        log('info', logSystem, 'Payments sent via wallet daemon \n %j', command.rpc.transaction.transfers);
        redisClient.multi(command.redis).exec(function (error, replies) {
        if (error) {
          log('error', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
          log('error', logSystem, 'Double payments likely to be sent to %j', command.rpc.destinations);
          cback(true);
          return;
        }

        for (var m in notify_miners_on_success) {
          notify_miners.push(notify_miners_on_success[m]);
        }
          cback(false);
          return;
        });
      }
    ], function (error, result) {
        if (error) {
          log('error', logSystem, 'TX error');
          if(result)
            log('error', logSystem, 'TX result: --> %j', [result]);
          cmd = 0;
					return callback(false,null);
        }

        for (var m in notify_miners) {
          var notify = notify_miners[m];
          log('info', logSystem, 'Payment of %s to %s', [ utils.getReadableCoins(notify.amount), notify.address ]);
          notifications.sendToMiner(notify.address, 'payment', {
          'ADDRESS': notify.address.substring(0,7)+'...'+notify.address.substring(notify.address.length-7),
          'AMOUNT': utils.getReadableCoins(notify.amount),
          });
        }//for

        log('info', logSystem, 'TX sent');

				Pay(transferCommands,callback);
    });
}//

function createTransaction(command, callback) {
  if(lock){
    callback(true, "payment lock [createTransaction]");
    return;
  }
  else {
    lock = true;
  }
  apiInterfaces.rpcWallet('create_transaction', command.rpc, function (error, result) {
      if (error || !result) {
          log('error', logSystem, 'Error with create_transaction %j', [error]);
          log('error', logSystem, 'TX: %j', command.rpc.transaction.transfers);
					lock = false;
          callback(error||true, result || 'Error with create_transaction');
          return;
      }

      command.tx.binary_transaction = result.binary_transaction;
      command.mixin = result.transaction.anonymity;
      command.fee = result.transaction.fee;
      command.hash = result.transaction.hash;
      command.created = true;

      log('info', logSystem, 'create_transaction ok, command.rpc: %j', [command.rpc]);
      //log('info', logSystem, 'create_transaction ok, result: %j', [result]);

      callback(false, command);
    })
}

function sendTransaction(command, callback) {
    if (!command.created) {
			  log('error', logSystem, 'Error with send_transaction, command not created!');
				lock = false;
        callback(true, command);
        return;
    }

    apiInterfaces.rpcWallet('send_transaction', command.tx, function (error, result) {
      lock = false;
      if (error || !result) {
          log('error', logSystem, 'Error with send_transaction RPC request to wallet daemon %j', [error]);
          log('error', logSystem, 'Payments failed to send to %j', command.rpc.transaction.transfers);
          callback(error||true, command.tx);
          return;
      }

      if (result.send_result != "broadcast") {
          log('error', logSystem, 'Error with send_transaction RPC request to wallet daemon %j', [result]);
          log('error', logSystem, 'Payments failed to send to %j', [command.rpc.transaction]);//command.rpc.transaction.transfers
          callback(error||true,result.send_result);
          return;
      }

      command.sent = true;

      //log('info', logSystem, 'send_transaction ok command.tx: %j', [command.tx]);
      log('info', logSystem, 'send_transaction ok result: %j', [result]);

      callback(false, command);
    });
}

function runInterval () {
	async.waterfall([

		// Get worker keys
		function (callback) {
			redisClient.keys(config.coin + ':workers:*', function (error, result) {
				if (error) {
					log('error', logSystem, 'Error trying to get worker balances from redis %j', [error]);
					callback(true);
					return;
				}
				callback(null, result);
			});
		},

		// Get worker balances
		function (keys, callback) {
			var redisCommands = keys.map(function (k) {
				return ['hget', k, 'balance'];
			});
			redisClient.multi(redisCommands)
				.exec(function (error, replies) {
					if (error) {
						log('error', logSystem, 'Error with getting balances from redis %j', [error]);
						callback(true);
						return;
					}

					var balances = {};
					for (var i = 0; i < replies.length; i++) {
						var parts = keys[i].split(':');
						var workerId = parts[parts.length - 1];

						balances[workerId] = parseInt(replies[i]) || 0;
					}
					callback(null, keys, balances);
				});
		},

		// Get worker minimum payout
		function (keys, balances, callback) {
			var redisCommands = keys.map(function (k) {
				return ['hget', k, 'minPayoutLevel'];
			});
			redisClient.multi(redisCommands)
				.exec(function (error, replies) {
					if (error) {
						log('error', logSystem, 'Error with getting minimum payout from redis %j', [error]);
						callback(true);
						return;
					}

					var minPayoutLevel = {};
					for (var i = 0; i < replies.length; i++) {
						var parts = keys[i].split(':');
						var workerId = parts[parts.length - 1];

						var minLevel = config.payments.minPayment;
						var maxLevel = config.payments.maxPayment;
						var defaultLevel = minLevel;

						var payoutLevel = parseInt(replies[i]) || minLevel;
						if (payoutLevel < minLevel) payoutLevel = minLevel;
						if (maxLevel && payoutLevel > maxLevel) payoutLevel = maxLevel;
						minPayoutLevel[workerId] = payoutLevel;

						if (payoutLevel !== defaultLevel) {
							log('info', logSystem, 'Using payout level of %s for %s (default: %s)', [utils.getReadableCoins(minPayoutLevel[workerId]), workerId, utils.getReadableCoins(defaultLevel)]);
						}
					}
					callback(null, balances, minPayoutLevel);
				});
		},

		// Filter workers under balance threshold for payment
		function (balances, minPayoutLevel, callback) {
			var payments = {};

			for (var worker in balances) {
				var balance = balances[worker];
				if (balance >= minPayoutLevel[worker]) {
					var remainder = balance % config.payments.denomination;
					var payout = balance - remainder;

					if (payout < 0) continue;

					payments[worker] = payout;
				}
			}

			if (Object.keys(payments)
				.length === 0) {
				log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
				callback(true);
				return;
			}

			var transferCommands = [];
			var addresses = 0;
			var commandAmount = 0;
			var commandIndex = 0;

			for (var worker in payments) {
				var amount = parseInt(payments[worker]);
				if (config.payments.maxTransactionAmount && amount + commandAmount > config.payments.maxTransactionAmount) {
					amount = config.payments.maxTransactionAmount - commandAmount;
				}

				var address = worker;
				var payment_id = null;

				var with_payment_id = false;

				var addr = address.split(config.poolServer.paymentId.addressSeparator);
				if ((addr.length === 1 && utils.isIntegratedAddress(address)) || addr.length >= 2) {
					with_payment_id = true;
					if (addr.length >= 2) {
						address = addr[0];
						payment_id = addr[1];
						payment_id = payment_id.replace(/[^A-Za-z0-9]/g, '');
						if (payment_id.length !== 16 && payment_id.length !== 64) {
							with_payment_id = false;
							payment_id = null;
						}
					}
					if (addresses > 0) {
						commandIndex++;
						addresses = 0;
						commandAmount = 0;
					}
				}

				if (config.poolServer.fixedDiff && config.poolServer.fixedDiff.enabled) {
					var addr = address.split(config.poolServer.fixedDiff.addressSeparator);
					if (addr.length >= 2) address = addr[0];
				}

				if(!transferCommands[commandIndex]) {
          transferCommands[commandIndex] = {
                        redis: [],
                        amount: 0,
                        tx: {
                            binary_transaction: ""
                        },
                        hash: "",
                        mixin: 0,
                        fee: 0,
                        unlock_time: 0,
                        created: false,
                        sent: false,
                        rpc: {
                            // fee_per_byte: 1,
                            transaction:
                                {
                                    anonymity: config.payments.mixin,
                                    payment_id: "", //ASCII https://pool.armor
                                    transfers: [],
                                },

                            optimization: "aggressive", /* minimal */
                            save_history: false,
                            subtract_fee_from_amount: true,
                            spend_addresses: [config.poolServer.poolAddress],
                            change_address: config.poolServer.poolAddress
        }
          };
        }

				transferCommands[commandIndex].rpc.transaction.transfers.push({ amount: amount, address: address });

				if (payment_id) transferCommands[commandIndex].rpc.transaction.payment_id = payment_id;

				transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
        if(!config.payments.minerPayFee){
            transferCommands[commandIndex].rpc.subtract_fee_from_amount = false;
        }
				transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
				transferCommands[commandIndex].amount += amount;

				addresses++;
				commandAmount += amount;

				if (addresses >= config.payments.maxAddresses || (config.payments.maxTransactionAmount && commandAmount >= config.payments.maxTransactionAmount) || with_payment_id) {
					commandIndex++;
					addresses = 0;
					commandAmount = 0;
				}
			}
      //for worker in payments
      callback(false,transferCommands);
		},
    function(command,callback){
      Pay(command, function(error,result){
        log('info', logSystem, 'Function Pay executed.');
        callback(error,result);
      });
    }

	], function (error, result) {
		setTimeout(runInterval, config.payments.interval * 1000);
	});
}

runInterval();
