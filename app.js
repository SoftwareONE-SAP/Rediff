/**
 * Application for diffing two instances of Redis
 * @author William Overton <woverton@centiq.co.uk>
 * @copyright Centiq Ltd.
 */

/**
 * Load librarys
 */
var redis = require("redis");
var clc = require("cli-color");
var stdio = require('stdio');

/**
 * Our controller for diffing redis
 */
var Rediff = function(){
	var parent = this;

	/**
	 * The keys for client A and B and Unique and Shared and Different
	 * @type {Array}
	 */
	this.ka = [];//ClientA
	this.kb = [];//ClientB
	this.ku = [];//Unique
	this.ks = [];//Shared
	this.kd = [];//Different

	/**
	 * Load our utility class
	 * @type {RediffUtils}
	 */
	this.utils = new RediffUtils();

	this.log = this.utils.log.bind(this);

	/**
	 * Init our connection details
	 */
	this.init();

	this.conn = new RediffConnectionManager(this.InstanceA, this.InstancePortA, this.InstanceB, this.InstancePortB, this.poolCount);

	/**
	 * Connect to our instances then carry on
	 * @return {void}
	 */
	parent.log(clc.white("Connecting..."));
	this.conn.connect.bind(this.conn)(function(){

		/**
		 * Get all keys from both instances
		 */
		parent.getKeysForBothClients.bind(parent)(function(){

			/**
			 * Find unique keys
			 */
			parent.log(clc.white("Sorting unique keys..."));
			parent.ku = parent.utils.getDiffKeys(parent.ka, parent.kb);

			/**
			 * If the user requested to see the unique keys, show them
			 */
			if(parent.ops['output-unique']){
				parent.outputUniqueKeys.bind(parent)();
			}

			/**
			 * Find shared keys
			 */
			parent.log(clc.white("Sorting shared keys..."));
			parent.ks = parent.utils.getSharedKeys(parent.ka, parent.kb, parent.ku);

			/**
			 * If the user requested to see the shared keys, show them
			 */
			if(parent.ops['output-shared']){
				parent.outputSharedKeys.bind(parent)();
			}

			/**
			 * Compute which keys have different values on each side, then call callback
			 */
			parent.log(clc.white("Checking values for keys..."));
			parent.computeDifferentKeys.bind(parent)(function(){

				/**
				 * If the user requested to see the different keys, show them
				 */
				if(parent.ops['output-different']){
					parent.outputDifferentKeys.bind(parent)();
				}

				/**
				 * Write to clientB if requested
				 */
				if(parent.ops['write']){

					parent.writeAToClientB.bind(parent)(function(){
						/**
						 * We're done here
						 */
						parent.complete();
					});

				}else{
					/**
					 * We're done here
					 */
					parent.complete();
				}
				
			});

		});
	});
}

/**
 * Load variables and configure connection details
 * @return {void}
 */
Rediff.prototype.init = function(){
	/**
	 * Get cli parameters
	 * @type {Object}
	 */
	this.ops = stdio.getopt({
	    'clienta': {key: 'a', args: 1, description: 'The first host to connect to'},
	    'clientb': {key: 'b', args: 1, description: 'The second host to connect to'},
	    'pool': {key: 'p', args: 1, description: 'The amount of connections to open'},
	    'interval': {key: 'i', args: 1, description: 'The time to space out requests by'},
	    'output-unique': {key: 'u', args: 0, description: 'Should output unique'},
	    'output-shared': {key: 's', args: 0, description: 'Should output shared keys'},
	    'output-different': {key: 'd', args: 0, description: 'Should output different keys'},
	    'quiet': {key: 'q', args: 0, description: 'Only output data'},
	    'write': {key: 'w', args: 0, description: 'OVERWRITE clientb with clienta'},
	});

	/**
	 * Get connection details from cli parameters
	 * @type {String}
	 */
	this.InstanceA = this.ops['clienta'].split(":")[0];
	this.InstanceB = this.ops['clientb'].split(":")[0];

	this.poolCount = this.ops.pool;

	/**
	 * If there is a port specified, override the default
	 */
	if(this.ops['clienta'].split(":").length > 1){
		this.InstancePortA = ops['clienta'].split(":")[1];
	}

	if(this.ops['clientb'].split(":").length > 1){
		this.InstancePortB = ops['clientb'].split(":")[1];
	}
}

/**
 * Write all data from A to B and remove unique data from B
 * @param  {Function} callback Called when done
 * @return {void}
 */
Rediff.prototype.writeAToClientB = function(callback){
	var parent = this;

	var uniqueDone = 0;
	var differentDone = 0;

	var complete = function(){
		var mes = clc.green(uniqueDone) + clc.blueBright(" / ") + clc.green((parent.ku.length) + " unique keys synced");
		parent.log(mes);
		var mes = clc.green(differentDone) + clc.blueBright(" / ") + clc.green((parent.kd.length) + " different keys synced");
		parent.log(mes);
		process.stdout.write(clc.up(2));

		if(uniqueDone >= parent.ku.length && differentDone >= parent.kd.length){
			process.stdout.write(clc.down(2));
			callback();
			return;
		}
	}

	/**
	 * If we have no unique or different then lets get out of here
	 */
	if(parent.ku.length == 0 && parent.kd.length == 0){
		complete();
	}

	/**
	 * Loop through all unique keys and remove them from B and copy keys from A to B
	 * @param  {String} key   The Redis key
	 * @param  {Integer} index The index of the key in our keys array
	 * @param  {Array} array The array being traversed
	 * @return {void}
	 */
	parent.ku.forEach(function(key, index, array){

		/**
		 * Is key unique to B?
		 */
		if(parent.kb.indexOf(key) > -1){

			/**
			 * Remove unique keys from B
			 */
			parent.conn.getClient("b").del(key, function(err, data){
				uniqueDone++;
				complete();
			});

		}else{

			parent.utils.copyClientaKeyToClientB.bind(parent)(key, function(){
				uniqueDone++;
				complete();
			});
		}

	});

	parent.kd.forEach(function(key, index, array){

		/**
		 * OVERWRITE different keys
		 */
		parent.conn.getClient("b").del(key, function(err, data){
			parent.utils.copyClientaKeyToClientB.bind(parent)(key, function(){
				differentDone++;
				complete();
			});
		});

	});
}

/**
 * Output all keys that have different data to the other side's set
 * @return {void}
 */
Rediff.prototype.outputDifferentKeys = function(){
	var parent = this;

	for (var i = parent.kd.length - 1; i >= 0; i--) {

		parent.log(clc.yellow("[DIFFERENT]") + " -> " + clc.cyan(parent.kd[i]), true);

	};
}

Rediff.prototype.computeDifferentKeys = function(callback){
	var parent = this;

	if(parent.ks.length == 0){
		callback();
		return;
	}

	var done = 0;

	var imdone = function(){
		done++;
		if(done >= parent.ks.length - 1){
			callback();
			return;
		}else{
			var mes = clc.green(done) + clc.blueBright(" / ") + clc.green((this.ks.length - 1) + " keys");
			parent.log(mes);
			process.stdout.write(clc.up(1));
		}
	}

	for (var i = parent.ks.length - 1; i >= 0; i--) {

		var delay = parseInt(this.ops['interval']) * i;
		
		setTimeout(function(key){

			parent.utils.getDataForKeyBoth(parent.conn, key, function(adata, bdata){
				if(!parent.utils.valuesIdentical.bind(parent)(adata, bdata, key)){
					parent.kd.push(key);
				}

				imdone.bind(parent)();
			});

		}, delay, parent.ks[i]);

	};
}

/**
 * Output all keys that are on both sides
 * @return {void}
 */
Rediff.prototype.outputSharedKeys = function(){
	var parent = this;

	for (var i = parent.ks.length - 1; i >= 0; i--) {

		parent.log(clc.blueBright("[SHARED]") + " -> " + clc.cyan(parent.ks[i]), true);

	};
}

/**
 * Figure out and output the unique keys
 * @return {void}
 */
Rediff.prototype.outputUniqueKeys = function(){
	var parent = this;

	for (var i = parent.ku.length - 1; i >= 0; i--) {
		var location = "";

		if(parent.ka.indexOf(parent.ku[i]) > -1){
			location = "ClientA";
		}else{
			location = "ClientB";
		}

		parent.log(clc.red("[UNIQUE]") + " -> " + location + " -> " + clc.cyan(parent.ku[i]), true);
	}
}

Rediff.prototype.complete = function(){
	var parent = this;

	parent.log(clc.white("--------------------------------------------------"));
	parent.log(clc.green("[DONE]"));
	parent.log(clc.greenBright("Total keys found: " + (this.ks.length + this.ku.length)));
	parent.log(clc.greenBright("Total shared keys found: " + this.ks.length));
	parent.log(clc.greenBright("Total different keys: " + this.kd.length));
	parent.log(clc.greenBright("Total unique keys: " + this.ku.length));
	process.exit();
};

Rediff.prototype.getKeysForBothClients = function(callback){
	var parent = this;

	var adone = false;
	var bdone = false;

	parent.log(clc.white("Collecting keys from both instances"));

	/**
	 * Return when both async tasks are done
	 * @param  {String} namespace the namespace of the async task
	 * @return {void}           Calls a callback and then returns void
	 */
	var imdone = function(namespace){
		if(namespace == "a"){
			adone = true;
		}

		if(namespace == "b"){
			bdone = true;
		}

		if(adone && bdone){
			callback();
			return;
		}
	}

	/**
	 * Get keys for clienta
	 */
	parent.utils.getKeys(parent.conn.getClient("a"), function(err, data){
		if(err){
			parent.log("Error getting keys for clienta");
		}

		parent.log(clc.white("Collected keys from clienta"));
		parent.ka = data;

		/**
		 * A is done!
		 */
		imdone("a");
	});

	/**
	 * Get keys for clientb
	 */
	parent.utils.getKeys(parent.conn.getClient("b"), function(err, data){
		if(err){
			parent.log("Error getting keys for clientb");
		}

		parent.log(clc.white("Collected keys from clientb"));
		parent.kb = data;

		/**
		 * B is done!
		 */
		imdone("b");
	});
}

var RediffUtils = function(){

}

RediffUtils.prototype.copyClientaKeyToClientB = function(key, callback){
	var parent = this;

	/**
	 * Write unique keys from A -> B
	 */
	parent.utils.getDataTypeOfUniqueKey(parent.conn.getClient("a"), key, function(type){
		switch(type){

			case "string":

				/**
				 * Use the key to get data from A
				 * @param  {String} err  Status of call
				 * @param  {String} data The data of the key in A
				 * @return {void}
				 */
				parent.conn.getClient("a").get(key, function(err, data){

					/**
					 * Write the data for the key from A into B.
					 * @param  {String} err  Status of call
					 * @param  {String} data The updated record
					 * @return {void}
					 */
					parent.conn.getClient("b").set(key, data, function(err, data){
						callback();
						return;
					});
				});
				
				break;

			case "list":

				parent.conn.getClient("a").lrange(key, 0, -1, function(err, data){

					/**
					 * How many requests have completed
					 * @type {Number}
					 */
					var count = 0;

					/**
					 * One all requests are done, call complete
					 * @return {void} Calls complete once this key is done with
					 */
					var imdone = function(){
						count++;
						if(count >= data.length){
							callback();
							return;
						}
					}

					/**
					 * Push each element into list
					 * @TODO there has to be a better way......
					 */
					for(var i = 0 ; i < data.length ; i ++){
						parent.conn.getClient("b").rpush(key, data[i], function(){
							imdone();
						});
					}
				});
				

				break;

			case "set":

				parent.conn.getClient("a").smembers(key, function(err, data){

					/**
					 * How many requests have completed
					 * @type {Number}
					 */
					var count = 0;

					/**
					 * One all requests are done, call complete
					 * @return {void} Calls complete once this key is done with
					 */
					var imdone = function(){
						count++;
						if(count >= data.length){
							callback();
							return;
						}
					}

					for(var i = 0 ; i < data.length ; i ++){
						parent.conn.getClient("b").sadd(key, data[i], function(err, data){
							imdone();
						});
					}

				});

				break;

			case "hash":

				/**
				 * Get all data for a hash from A
				 * @param  {String} err  Status of call
				 * @param  {Object} data the hash from A
				 * @return {void}
				 */
				parent.conn.getClient("a").hgetall(key, function(err, data){
					/**
					 * The keys for our data
					 * @type {Array}
					 */
					var keys = Object.keys(data);

					/**
					 * How many requests have completed
					 * @type {Number}
					 */
					var count = 0;

					/**
					 * One all requests are done, call complete
					 * @return {void} Calls complete once this key is done with
					 */
					var imdone = function(){
						count++;
						if(count >= keys.length){
							callback();
							return;
						}
					}

					for(var i = 0 ; i < keys.length; i++){
						parent.conn.getClient("b").hset(key, keys[i], data[keys[i]], function(err, data){
							imdone();
						});
					}

				});

				break;

			default:

				parent.log(clc.red("[ISSUE]") + " Type of key could not be defined");

				callback();
				return;

				break;
		}
	});
}

RediffUtils.prototype.valuesIdentical = function(adata, bdata, key){
	var parent = this;

	if(typeof adata == "string" && typeof bdata == "string"){
		if(adata == bdata){
			return true;
		}else{
			return false;
		}
	}

	if(adata instanceof Array && bdata instanceof Array){

		if(adata.length != bdata.length){
			return false;
		}

		adata = adata.sort();
		bdata = bdata.sort();

		for(var i = 0 ; i < adata.length ; i ++){
			if(adata[i] != bdata[i]){
				return false;
			}
		}

		return true;
	}

	if(adata instanceof Object && bdata instanceof Object){
		var keysa = Object.keys(adata).sort().slice(0);
		var keysb = Object.keys(bdata).sort().slice(0);

		var diffKeys = this.utils.getDiffKeys(keysa.slice(0), keysb.slice(0));

		if(diffKeys.length > 0){
			return false;
		}

		for(var i = 0 ; i < keysa.length ; i++){
			var apart = adata[keysa[i]];
			var bpart = bdata[keysa[i]];

			if(apart != bpart){
				return false;
			}
		}

		return true;

	}

	parent.log(clc.red("[ISSUE]") + " Not caught Key: '" + key + "', Type -> (a: '" + typeof adata + "' b: '" + typeof adata + "') Data: " + JSON.stringify(adata), true);

	return false;
}

RediffUtils.prototype.getKeys = function(client, callback){
	client.keys("*", callback);
}

/**
 * get keys that only exist in one but not both
 * @param  {Array} ka The keys for clienta
 * @param  {Array} kb the keys for client b
 * @return {Array}    and array of unique keys
 * @note http://stackoverflow.com/questions/1187518/javascript-array-difference
 */
RediffUtils.prototype.getDiffKeys = function(ka, kb){
	var tmp = [];
	var diff=[];

	/**
	 * Set all keys for ka to true
	 */
	for(var i=0;i<ka.length;i++){
		tmp[ka[i]]=true;
	}
	
	/**
	 * Remove all the keys from ka if it exists in kb. If it does not exist in ka add it to tmp
	 */
	for(var i=0;i<kb.length;i++){

		if(tmp[kb[i]]){
			delete tmp[kb[i]];
		}else{
			tmp[kb[i]]=true;
		}

	}

	for(var k in tmp){
		diff.push(k);
	}

	return diff;

}

/**
 * Get all keys which are in both ka and kb
 * @param  {Array} ka Keys from client A
 * @param  {Array} kb Keys from client B
 * @param  {Array} ku Known Unique Keys
 * @return {Array}    The keys that both clients have
 */
RediffUtils.prototype.getSharedKeys = function(ka, kb, ku){

	var ks = this.arrayUnique(ka.concat(kb));

	for(i = 0; i < ku.length; i++){
		var index = ks.indexOf(ku[i]);

		if(index > -1){
			ks.splice(index, 1);
		}
	}

	return ks;
}

/**
 * Remove all duplicates from an array
 * @param  {Array} array the array to remove dupes from
 * @return {Array}       The array with no duplicates
 * @note http://stackoverflow.com/questions/1584370/how-to-merge-two-arrays-in-javascript-and-de-duplicate-items
 */
RediffUtils.prototype.arrayUnique = function(array) {
    var a = array.concat();
    for(var i=0; i<a.length; ++i) {
        for(var j=i+1; j<a.length; ++j) {
            if(a[i] === a[j])
                a.splice(j--, 1);
        }
    }

    return a;
};

RediffUtils.prototype.getDataForKeyBoth = function(conn, key, callback){

	var adone = false;
	var bdone = false;

	var adata = null;
	var bdata = null;

	/**
	 * Return when both async tasks are done
	 * @param  {String} namespace the namespace of the async task
	 * @return {void}           Calls a callback and then returns void
	 */
	var imdone = function(namespace){
		if(namespace == "a"){
			adone = true;
		}

		if(namespace == "b"){
			bdone = true;
		}

		if(adone && bdone){
			callback(adata, bdata);
			return;
		}
	}

	var method = this.getDataTypeOfKey(conn, key, function(method){

		switch(method){

			case "string":

				conn.getClient("a").get(key, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					adata = data;

					imdone("a");
				});

				conn.getClient("b").get(key, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					bdata = data;

					imdone("b");
				});

				break;

			case "list":

				conn.getClient("a").lrange(key, 0, 999999, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					adata = data;

					imdone("a");
				});

				conn.getClient("b").lrange(key, 0, 999999, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					bdata = data;

					imdone("b");
				});

				break;

			case "set":

				conn.getClient("a").smembers(key, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}

					adata = data;

					imdone("a");
				});

				conn.getClient("b").smembers(key, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					bdata = data;

					imdone("b");
				});

				break;

			case "hash":

				conn.getClient("a").hgetall(key, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					adata = data;

					imdone("a");
				});

				conn.getClient("b").hgetall(key, function(err, data){
					if(err){
						parent.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					bdata = data;

					imdone("b");
				});


			break;

			case "DIFFERENT":

				adata = 1;
				bdata = 2;
				imdone("a");
				imdone("b");

			break;

			default:

				break;
		}
	});

}

RediffUtils.prototype.log = function(message, isData){
	if(!this.ops.quiet || isData)
	console.log(message);
}

RediffUtils.prototype.getDataTypeOfKey = function(conn, key, callback){
	var parent = this;

	var adone = false;
	var bdone = false;

	var adata = null;
	var bdata = null;

	/**
	 * Return when both async tasks are done
	 * @param  {String} namespace the namespace of the async task
	 * @return {void}           Calls a callback and then returns void
	 */
	var imdone = function(namespace){
		if(namespace == "a"){
			adone = true;
		}

		if(namespace == "b"){
			bdone = true;
		}

		if(adone && bdone){
			var type = adata + "";

			if(type != bdata){
				type = "DIFFERENT"
			}

			callback(type);
			return;
		}
	}

	conn.getClient("a").type(key, function(err, data){
		adata = data;

		imdone("a");
	});

	conn.getClient("b").type(key, function(err, data){
		bdata = data;

		imdone("b");
	});
}

RediffUtils.prototype.getDataTypeOfUniqueKey = function(client, key, callback){
	var parent = this;

	client.type(key, function(err, data){
		callback(data);
		return;
	});
}

var RediffConnectionManager = function(hostA, portA, hostB, portB, poolCount){
	/**
	 * Our connection Details
	 * @type {String}
	 */
	this.hostA = hostA;
	this.portA = portA;
	this.hostB = hostB;
	this.portB = portB;

	/**
	 * How many connections to open
	 * @type {Interger}
	 */
	this.poolCount = poolCount;

	this.clients = {
		a: [],
		b: []
	}
}

RediffConnectionManager.prototype.connect = function(callback){
	var parent = this;

	/**
	 * How many have connected
	 * @type {Number}
	 */
	var adone = 0;
	var bdone = 0;

	var imdone = function(namespace){
		
		if(namespace == "a"){
			adone++;
		}

		if(namespace == "b"){
			bdone++;
		}
		
		if(adone >= parent.poolCount && bdone >= parent.poolCount){

			/**
			 * Clear up output
			 */
			console.log("                                                          ");
			console.log("                                                          ");
			process.stdout.write(clc.up(2));

			/**
			 * Return
			 */
			callback();
			return;
		}else{
			/**
			 * Output a progress message
			 */
			var mes = clc.green(adone) + clc.blueBright(" / ") + clc.green((parent.poolCount) + " clienta connected");
			console.log(mes);
			var mes = clc.green(bdone) + clc.blueBright(" / ") + clc.green((parent.poolCount) + " clientb connected");
			console.log(mes);
			process.stdout.write(clc.up(2));
		}
	}

	for (var i = this.poolCount - 1; i >= 0; i--) {
		this.clients.a[i] = redis.createClient(this.portA, this.hostA);
		this.clients.b[i] = redis.createClient(this.portB, this.hostB);

		this.clients.a[i].on("error", function (err) {
	        console.log("ClientA: " + err);
	        process.exit();
	    });

	    this.clients.b[i].on("error", function (err) {
	        console.log("ClientB: " + err);
	        process.exit();
	    });

	    this.clients.a[i].on("ready", function (err) {
	    	if(err){
	    		console.log("ClientA: " + err);
	    		return;
	    	}

	        imdone("a");
	    });

	    this.clients.b[i].on("ready", function (err) {
	        if(err){
	    		console.log("ClientB: " + err);
	    		return;
	    	}

	        imdone("b");
	    });
	};
}

RediffConnectionManager.prototype.getClient = function(namespace){
	return this.clients[namespace][parseInt(Math.random() * this.clients[namespace].length)];
}

var rediff = new Rediff();

