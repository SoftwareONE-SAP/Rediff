/**
 * Application for diffing two instances of Redis
 * @author William Overton <woverton@centiq.co.uk>
 * @copyright Centiq Ltd.
 */

/**
 * Connect to redis
 */
var redis = require("redis");
var clc = require("cli-color");
var stdio = require('stdio');

var InstanceA = "";
var InstanceB = "";

var ops = stdio.getopt({
    'clienta': {key: 'a', args: 1, description: 'The first host to connect to'},
    'clientb': {key: 'b', args: 1, description: 'The second host to connect to'},
    'delay': {key: 'd', args: 1, description: 'The time to space out requests by'}
});

InstanceA = ops['clienta'];
InstanceB = ops['clientb'];

/**
 * Our controller for diffing tools
 */
var Rediffs = function(){
	var parent = this;

	if(InstanceA.length == 0 || InstanceB.length == 0){
		console.log(clc.redBright("You did not specify valid hosts"));
		process.exit();
	}

	this.utils = new RediffsUtils();

	console.log(clc.white("Connecting"));

	this.clienta = redis.createClient("6379", InstanceA);
	this.clientb = redis.createClient("6379", InstanceB);

	this.clienta.on("error", function (err) {
        console.log("ClientA: " + err);
    });

    this.clientb.on("error", function (err) {
        console.log("ClientB: " + err);
    });

    this.clienta.on("ready", function (err) {
    	if(err){
    		console.log("ClientA: " + err);
    		return;
    	}
        console.log(clc.white("ClientA Connected: "));
    });

    this.clientb.on("ready", function (err) {
        if(err){
    		console.log("ClientB: " + err);
    		return;
    	}
        console.log(clc.white("ClientB Connected: "));
    });

	/**
	 * The keys for client A and B and Unique and Shared
	 * @type {Array}
	 */
	this.ka = [];
	this.kb = [];
	this.ku = [];
	this.ks = [];
	parent.kd = 0;

	this.completed = 0;

	/**
	 * Get all keys from both instances
	 */
	this.getKeysForBothClients.bind(this)(function(){
		console.log(clc.white("Sorting keys.."));

		/**
		 * Sort the keys alphabetically
		 */
		parent.ka.sort();
		parent.kb.sort();

		/**
		 * Find unique keys
		 */
		parent.ku = parent.utils.getDiffKeys(parent.ka, parent.kb);

		/**
		 * Find ahred keys
		 */
		parent.ks = parent.utils.getSharedKeys(parent.ka, parent.kb, parent.ku);

		for (var i = parent.ku.length - 1; i >= 0; i--) {
			var location = "";

			if(parent.ka.indexOf(parent.ku[i]) > -1){
				location = InstanceA;
			}else{
				location = InstanceB;
			}

			console.log(clc.red("[UNIQUE]") + " -> " + location + " -> " + clc.cyan(parent.ku[i]));
		}

		// console.log(JSON.stringify(parent.ks));

		for (var i = parent.ks.length - 1; i >= 0; i--) {

			var delay = parseInt(ops['delay']) * i;
			
			setTimeout(function(key){

				parent.utils.getDataForKeyBoth(parent.clienta, parent.clientb, key, function(adata, bdata){
					if(!parent.utils.valuesIdentical(adata, bdata)){
						parent.kd++;
						console.log(clc.yellow("[DIFFERENT]") + " -> " + clc.cyan(key));
					}

					parent.complete.bind(parent)();
				});

			}, delay, parent.ks[i]);

		};

	});
}

Rediffs.prototype.complete = function(){
	this.completed++;
	if(this.completed >= this.ks.length - 1){
		console.log(clc.white("--------------------------------------------------"));
		console.log(clc.green("[DONE]"));
		console.log(clc.greenBright("Total keys checked: " + this.ks.length));
		console.log(clc.greenBright("Total unique keys: " + this.ku.length));
		console.log(clc.greenBright("Total different keys: " + this.kd));
		process.exit();
	}else{
		var mes = clc.green(this.completed) + clc.blueBright(" / ") + clc.green((this.ks.length - 1) + " keys");
		console.log(mes);
		process.stdout.write(clc.up(1));
	}
};

Rediffs.prototype.getKeysForBothClients = function(callback){
	var parent = this;

	var adone = false;
	var bdone = false;

	console.log(clc.white("Collecting keys from both instances"));

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
	parent.utils.getKeys(parent.clienta, function(err, data){
		if(err){
			console.log("Error getting keys for clienta");
		}

		console.log(clc.white("Collected keys from clienta"));
		parent.ka = data;

		/**
		 * A is done!
		 */
		imdone("a");
	});

	/**
	 * Get keys for clientb
	 */
	parent.utils.getKeys(parent.clientb, function(err, data){
		if(err){
			console.log("Error getting keys for clientb");
		}

		console.log(clc.white("Collected keys from clientb"));
		parent.kb = data;

		/**
		 * B is done!
		 */
		imdone("b");
	});
}

var RediffsUtils = function(){

}

RediffsUtils.prototype.valuesIdentical = function(adata, bdata){

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

		var diffKeys = this.getDiffKeys(keysa.slice(0), keysb.slice(0));

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

	if(adata == bdata){
		return true;
	}

	console.log(clc.red("[ISSUE]") + " Not caught type -> a: '" + typeof adata + "' b: '" + typeof adata + "' Data: " + JSON.stringify(adata));

	return false;
}

RediffsUtils.prototype.getKeys = function(client, callback){
	client.keys("*", callback);
}

/**
 * get keys that only exist in one but not both
 * @param  {Array} ka The keys for clienta
 * @param  {Array} kb the keys for client b
 * @return {Array}    and array of unique keys
 * @note http://stackoverflow.com/questions/1187518/javascript-array-difference
 */
RediffsUtils.prototype.getDiffKeys = function(ka, kb){
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
 * @param  {[type]} ka [description]
 * @param  {[type]} kb [description]
 * @return {[type]}    [description]
 */
RediffsUtils.prototype.getSharedKeys = function(ka, kb, ku){

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
RediffsUtils.prototype.arrayUnique = function(array) {
    var a = array.concat();
    for(var i=0; i<a.length; ++i) {
        for(var j=i+1; j<a.length; ++j) {
            if(a[i] === a[j])
                a.splice(j--, 1);
        }
    }

    return a;
};

RediffsUtils.prototype.getDataForKeyBoth = function(clienta, clientb, key, callback){

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

	var method = this.getDataTypeOfKey(clienta, clientb, key, function(method){

		switch(method){

			case "string":

				clienta.get(key, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					adata = data;

					imdone("a");
				});

				clientb.get(key, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					bdata = data;

					imdone("b");
				});

				break;

			case "list":

				clienta.lrange(key, 0, 999999, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					adata = data;

					imdone("a");
				});

				clientb.lrange(key, 0, 999999, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					bdata = data;

					imdone("b");
				});

				break;

			case "set":

				clienta.smembers(key, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}

					adata = data;

					imdone("a");
				});

				clientb.smembers(key, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					bdata = data;

					imdone("b");
				});

				break;

			case "hash":

				clienta.hgetall(key, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
					}
					
					adata = data;

					imdone("a");
				});

				clientb.hgetall(key, function(err, data){
					if(err){
						console.log("[" + method + "] for: '" + key + "' Reported: '" + err + "'");
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

RediffsUtils.prototype.getDataTypeOfKey = function(clienta, clientb, key, callback){
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

	clienta.type(key, function(err, data){
		adata = data;

		imdone("a");
	});

	clienta.type(key, function(err, data){
		bdata = data;

		imdone("b");
	});
}

var rediffs = new Rediffs();