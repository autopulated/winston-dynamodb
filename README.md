# DynamoDB Transport for Winston

A DynamoDB transport for [winston][0].

## Usage
```javascript
  var winston = require('winston');
  
  require('winston-dynamodb').DynamoDB;
  
  winston.add(winston.transports.DynamoDB, options);
```

## Options

```
accessKeyId            : an AWS access key id
secretAccessKey        : an AWS secret access key
region                 : the AWS region to use
tableName              : DynamoDB table name (this is automatically suffixed
                         with dates to per-week tables)
hostname               : hostname to use in logs (used as hash key for DB table)
provisionWriteCapacity : write capacity units to provision for active log table
provisionReadCapacity  : read capacity units to provision for active log table
```

## Prerequisite

The module sets up its own tables in DynamoDB (one per week). Logs are not
automatically deleted, you must drop old tables when you no longer wish to
access them.


## AWS Credentials

All of these options are values that you can find from your Amazon Web Services account: 'accessKeyId', 'secretAccessKey' and 'awsAccountId'.

## Installation

``` bash
  $ npm install winston
  $ npm install winston-dynamodb
```

#### Authors
[JeongWoo Chang](http://twitter.com/inspiredjw)
[James Crosby](http://twitter.com/autopulated)

[0]: https://github.com/flatiron/winston
