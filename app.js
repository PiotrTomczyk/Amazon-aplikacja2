var AWS = require("aws-sdk");
var helpers = require("./helpers");

var AWS_CONFIG_FILE = "./config.json";
var APP_CONFIG_FILE = "./app.json";
var haderr=false;
var fs = require('fs');






AWS.config.loadFromPath(AWS_CONFIG_FILE);
var appConfig = helpers.readJSONFile(APP_CONFIG_FILE);
var sqs = new AWS.SQS();
readMessage();
function readMessage(){
sqs.receiveMessage({
   QueueUrl: appConfig.QueueUrl,
   MaxNumberOfMessages: 1, // how many messages do we wanna retrieve?
   VisibilityTimeout: 60, // seconds - how long we want a lock on this job
   WaitTimeSeconds: 3 // seconds - how long should we wait for a message?
 }, function(err, data) {
   if (data.Messages) {
      var message = data.Messages[0],
           body = message.Body;
		   console.log(body);
		   var temp = body.split("#");
		   var action = temp[0];
		   var val = temp[1];
		   var key = temp[2];
		  
		  if(action=='rotate')  {
			path = downloadObject(key,function (err,path){
			if (err) console.log('nima');
			else {
			
			gm = require('gm');			
		gm(path)
		.rotate('green', val)
		.write(path, function (err) {
		if (err) console.log(err);
		else {
			sendObject(path,key);
		}
		});
			removeFromQueue(message);
		  }
		  })
}		
		if(action=='scale'){
			path = downloadObject(key,function (err,path){
				if (err) console.log('nima');
				else {
					//console.log(path)
					var par = val.split(',');
					var width = par[0];
					var height = par[1];
					gm = require('gm');			
					gm(path)
					.scale(width, height)
					.write(path, function (err) {
					if (err) console.log(err);
					else {
						sendObject(path,key);
					}
					});
				removeFromQueue(message);
				}
			})
		
		}
		if(action=='implode'){
			path = downloadObject(key,function (err,path){
				if (err) console.log('nima');
				else {
					
					gm = require('gm');			
					gm(path)
					.implode(val)
					.write(path, function (err) {
					if (err) console.log(err);
					else {
						sendObject(path,key);
					}
					});
				removeFromQueue(message);
				}
			})
		
		}
		if(action=='colorize'){
			path = downloadObject(key,function (err,path){
				if (err) console.log('nima');
				else {
					//console.log(path)
					var par = val.split(',');
					var red = par[0];
					var green = par[1];
					var blue = par[2];
					gm = require('gm');			
					gm(path)
					.colorize(red,green,blue)
					.write(path, function (err) {
					if (err) console.log(err);
					else {
						sendObject(path,key);
					}
					});
				removeFromQueue(message);
				}
			})
		
		}
		  
		   
     
   }
   else {
   console.log('oczekuje');
    
   }
   readMessage();
 });
  
 };
 var removeFromQueue = function(message) {
   sqs.deleteMessage({
      QueueUrl: appConfig.QueueUrl,
      ReceiptHandle: message.ReceiptHandle
   }, function(err, data) {
      err && console.log(err);
   });
};
var downloadObject = function(key,callback){
	var s3 = new AWS.S3();
	var klucz = key;
	var opcje = {
		Bucket: 'piotrtomczyk',
		Key: klucz
	};
	var path ='./downloads/'+klucz.substring(8);
	 var file = require('fs').createWriteStream(path);
	var stream = s3.getObject(opcje).createReadStream().pipe(file);
	stream.on('error', function(err){
		haderr=true;
		message = "Blad sciagania pliku "+klucz.substring(8);
		callback(err, path);
	});
	stream.on('close', function(){
		if (!haderr) message = "Pobrano plik "+klucz.substring(8);
		callback(null, path);
	});

}
var sendObject = function(path,key){
	console.log(path);
	//var sc = path;
	
	var body = fs.createReadStream(path);
	console.log(path);
	var s3obj = new AWS.S3({params: {Bucket: 'piotrtomczyk', Key: key}});
	s3obj.upload({Body: body}).
	on('httpUploadProgress', function(evt) { console.log(evt); }).
	send(function(err, data) { console.log(err, data) });
  

}
























