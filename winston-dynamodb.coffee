winston = require "winston"
util = require "util"
AWS = require "aws-sdk"
uuidV4 = require("node-uuid").v4
_ = require "lodash"
hostname = require("os").hostname()
microtime = require('microtime')

# Return timestamp with YYYY-MM-DD HH:mm:ss.000000
formatTimestamp = (musecs) ->
    date = new Date musecs/1000
    date =
        year: date.getFullYear()
        month: date.getMonth() + 1
        day: date.getDate()

        hour: date.getHours()
        minute: date.getMinutes()
        second: date.getSeconds()
        musecs: musecs % 1000000

    keys = _.without Object.keys date, "year", "month", "day"
    date[key] = "0" + date[key] for key in keys when date[key] < 10
    while date.musecs.toString().length < 6
        date.musecs = "0" + date.musecs
    "#{date.year}-#{date.month}-#{date.day} #{date.hour}:#{date.minute}:#{date.second}.#{date.musecs}"

formatDate = (date) ->
    date =
        year: date.getFullYear()
        month: date.getMonth() + 1
        day: date.getDate()

    keys = _.without Object.keys date, "year", "month", "day"
    date[key] = "0" + date[key] for key in keys when date[key] < 10
    "#{date.year}.#{date.month}.#{date.day}"

DynamoDB = exports.DynamoDB = (options = {}) ->
    regions = [
        "us-east-1"
        "us-west-1"
        "us-west-2"
        "eu-west-1"
        "ap-northeast-1"
        "ap-southeast-1"
        "ap-southeast-2"
        "sa-east-1"
    ]

    unless options.accessKeyId?
        throw new Error "need accessKeyId"

    unless options.secretAccessKey?
        throw new Error "need secretAccessKey"

    unless options.region?
        throw new Error "need region"

    unless options.region in regions
        throw new Error "unavailable region given"

    unless options.tableName?
        throw new Error "need tableName"

    AWS.config.update
        accessKeyId: options.accessKeyId
        secretAccessKey: options.secretAccessKey
        region: options.region

    # override hostname if specified in options
    if'hostname' of options
        hostname = options.hostname

    # Winston Options
    @.name = "dynamodb"
    @.level = options.level or "info"

    # DynamoDB Options=
    @.db = new AWS.DynamoDB()
    @.region = options.region
    
    # a-z, A-Z, 0-9, _ (underscore), - (hyphen) and . (period)
    @.tableName = options.tableName

    if 'provisionReadCapacity' of options
        @.provisionReadCapacity  = options.provisionReadCapacity
    else
        @.provisionReadCapacity = 1

    if 'provisionWriteCapacity' of options
        @.provisionWriteCapacity = options.provisionWriteCapacity
    else
        @.provisionWriteCapacity = 1
    
    # make sure the table exists, and re-exec once per day
    @.ensureTables()
    setInterval(@.ensureTables.bind(@), (23+2*Math.random())*60*60*1000)

    return

util.inherits DynamoDB, winston.Transport

DynamoDB::log = (level, msg, meta, callback) ->
    # override hostname if specified in meta
    if 'hostname' of meta
        use_hostname = meta.hostname
    else
        use_hostname = hostname
    # DynamoDB Options
    params =
        TableName: (@.tableName + '.' + formatDate(mostRecentMonday()))
        Item:
            level:
                "S": level
            timestamp:
                "S": formatTimestamp microtime.now()
            msg:
                "S": msg
            hostname:
                "S": use_hostname

    params.Item.meta = "S": JSON.stringify meta if meta?
    
    @.db.client.putItem params, (err, data) =>
        if err
            Error.captureStackTrace(err)
            @.emit "error", err
        else
            @.emit "logged"

    callback null, true

mostRecentMonday = () ->
    today = new Date()
    diffToMonday = today.getDay() - 1
    if diffToMonday > 0
        diffToMonday -= 7
    return new Date(today.getTime() + diffToMonday*1000*60*60*24)

# ensure that a table tableName.YYYY.MM.DD exists for monday of this week and
# monday of next week (creating a table can take a few minutes, so best to do
# it ahead of time)
DynamoDB::ensureTables = () ->
    monday = mostRecentMonday()
    
    this_week_table = @.tableName + '.' + formatDate(monday)
    next_week_table = @.tableName + '.' + formatDate(new Date(monday.getTime() + 1000*60*60*24*7))
    #last_week_table = @.tableName + '.' + formatDate(new Date(next_monday.getTime() - 1000*60*60*24*7))

    require_tables = [
        this_week_table, next_week_table
    ]
    console.log('require_tables:', require_tables)
    found_tables = []
    # fat arrow = bind this (@)
    checkFoundTables = () =>
        console.log('check found tables:', found_tables)
        for tn in require_tables
            if not (tn in found_tables)
                @.createTable(tn)
                onTableActive = (err) =>
                    if err
                        Error.captureStackTrace(err)
                        @.emit "error", err
                        # if there's an error do retry, but not for a while
                        setTimeout(@.ensureTables, 15*60*1000)
                    @.ensureTables()
                # as soon as we've set one table to create, wait for it to be
                # created before trying to create any more
                # (can only create one table at any one time)
                return @.waitForTable(tn, onTableActive)

    addFoundTables = (err, data) =>
        if err
            Error.captureStackTrace(err)
            return @.emit "error", err
        console.log('found tables:', data)
        found_tables =  found_tables.concat(data.TableNames)
        if 'LastEvaluatedTableName' of data
            @.db.client.listTables({ExclusiveStartTableName:data.LastEvaluatedTableName},addFoundTables)
        else
            checkFoundTables()
    @.db.client.listTables({}, addFoundTables)

DynamoDB::createTable = (tableName) ->
    console.log("create table:", tableName)
    table_params =
        TableName:tableName
        AttributeDefinitions:[
           {AttributeName:'hostname',  AttributeType:'S'},
           {AttributeName:'timestamp', AttributeType:'S'},
           {AttributeName:'level',     AttributeType:'S'}
        ]
        KeySchema:[
           {AttributeName:'hostname',  KeyType:'HASH'},
           {AttributeName:'timestamp', KeyType:'RANGE'}
        ]
        LocalSecondaryIndexes:[{
            IndexName:'level-index',
            KeySchema:[
                {AttributeName:'hostname',  KeyType:'HASH'},
                {AttributeName:'level', KeyType:'RANGE'}
            ],
            Projection:{ProjectionType:'KEYS_ONLY'}}
        ]
        ProvisionedThroughput:
            ReadCapacityUnits:@.provisionWriteCapacity
            WriteCapacityUnits:@.provisionWriteCapacity
    @.db.client.createTable table_params, (err, data) =>
        if err
            Error.captureStackTrace(err)
            return @.emit "error", err

DynamoDB::waitForTable = (tableName, callback) ->
    @.db.client.describeTable {TableName:tableName}, (err, data) =>
        if err
            return callback(err)
        if data.TableStatus == "CREATING"
            # keep waiting
            onTimeout = () =>
                @.waitForTable(tableName, callback)
            setTimeout(onTimeout, 60*1000)
        else if data.TableStatus == "ACTIVE"
            return callback(null)
        else
            return callback("wait for table state:" + data.TableStatus)

# Add DynamoDB to the transports by winston
winston.transports.DynamoDB = DynamoDB
