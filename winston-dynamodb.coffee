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

    if options.provisionReadCapacity?
        @.provisionReadCapacity  = options.provisionReadCapacity
    else
        @.provisionReadCapacity = 1

    if options.provisionWriteCapacity?
        @.provisionWriteCapacity = options.provisionWriteCapacity
    else
        @.provisionWriteCapacity = 1
    
    # make sure the table exists, and re-exec every 6 hours +- 1h
    @.ensureTables()
    setInterval(@.ensureTables.bind(@), (5+2*Math.random())*60*60*1000)

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
        TableName: (@.tableName + '.' + formatDate(mostRecentMonday(new Date())))
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

# possibly returns today
mostRecentMonday = (today) ->
    diffToMonday = 1-today.getDay()
    if diffToMonday > 0
        diffToMonday -= 7
    return new Date(today.getFullYear(), today.getMonth(), today.getDate() + diffToMonday)

# ensure that a table tableName.YYYY.MM.DD exists for monday of this week and
# monday of next week (creating a table can take a few minutes, so best to do
# it ahead of time)
DynamoDB::ensureTables = () ->
    today       = new Date(new Date().getTime())
    monday      = mostRecentMonday(today)
    next_monday = new Date(monday.getTime() + 1000*60*60*24*7)
    last_monday = new Date(monday.getTime() - 1000*60*60*24*7)

    require_tables = []
    downgrade_tables = []

    this_week_table = @.tableName + '.' + formatDate(monday)
    require_tables.push(this_week_table)
    
    # prepare table for next week 12 hours in advance
    if next_monday - today < 12*60*60*1000
        next_week_table = @.tableName + '.' + formatDate(next_monday)
        require_tables.push(next_week_table)
    
    # after 12 hours into the new week
    if today - monday > 12*60*60*1000
        last_week_table = @.tableName + '.' + formatDate(last_monday)
        downgrade_tables.push(last_week_table)
    
    found_tables = []
    # fat arrow = bind this (@)
    checkFoundTables = () =>
        for tn in downgrade_tables
            if tn in found_tables
                @.downgradeTable(tn)
        for tn in require_tables
            if not (tn in found_tables)
                @.createTable(tn)
                onTableActive = (err) =>
                    if err
                        @.emit "error", err
                        # if there's an error do retry, but not for a while
                        setTimeout(@.ensureTables.bind(@), 15*60*1000)
                    @.ensureTables()
                # as soon as we've set one table to create, wait for it to be
                # created before trying to create any more
                # (can only create one table at any one time)
                return @.waitForTable(tn, onTableActive)

    addFoundTables = (err, data) =>
        if err
            return @.emit "error", err
        found_tables =  found_tables.concat(data.TableNames)
        if 'LastEvaluatedTableName' of data
            @.db.client.listTables({ExclusiveStartTableName:data.LastEvaluatedTableName},addFoundTables)
        else
            checkFoundTables()
    @.db.client.listTables({}, addFoundTables)

DynamoDB::downgradeTable = (tableName) ->
    set_rcap = 1
    set_wcap = 1
    @.db.client.describeTable {TableName:tableName}, (err, data) =>
        if err
            Error.captureStackTrace(err)
            return @.emit "error", err
        if data.Table.TableStatus == 'ACTIVE' and
            (data.Table.ProvisionedThroughput.ReadCapacityUnits != set_rcap or
             data.Table.ProvisionedThroughput.WriteCapacityUnits != set_wcap)
            params =
                TableName:tableName
                ProvisionedThroughput:
                    ReadCapacityUnits:set_rcap
                    WriteCapacityUnits:set_wcap
            @.db.client.updateTable params, (err, data) =>
                if err
                    Error.captureStackTrace(err)
                    return @.emit "error", err

DynamoDB::createTable = (tableName) ->
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
            ReadCapacityUnits:@.provisionReadCapacity
            WriteCapacityUnits:@.provisionWriteCapacity
    @.db.client.createTable table_params, (err, data) =>
        if err
            Error.captureStackTrace(err)
            return @.emit "error", err

DynamoDB::waitForTable = (tableName, callback) ->
    @.db.client.describeTable {TableName:tableName}, (err, data) =>
        if err
            Error.captureStackTrace(err)
            return callback(err)
        if data.Table.TableStatus == "CREATING"
            # keep waiting
            onTimeout = () =>
                @.waitForTable(tableName, callback)
            setTimeout(onTimeout, 60*1000)
        else if data.Table.TableStatus == "ACTIVE"
            return callback(null)
        else
            return callback(new Error("wait for table state:" + data.Table.TableStatus))

# Add DynamoDB to the transports by winston
winston.transports.DynamoDB = DynamoDB
