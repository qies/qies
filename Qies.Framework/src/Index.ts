'use strict';
import { SequenceNumberSyncronizer } from "./SequenceNumberSyncronizer";
import * as AWS from 'aws-sdk';
import {StreamMananger} from "./StreamManager";

var dynamodb = new AWS.DynamoDB();
var sequenceNumberSyncronizer = new SequenceNumberSyncronizer(dynamodb);
var streamManager = new StreamMananger(dynamodb);

exports.handler = async (event: any, context: any, callback: any) => {
    var insertRecords = event.Records.filter((record) => record.eventName === "INSERT");
    if (insertRecords.length === 0)
        return;

    var executedCount = 0;

    insertRecords.forEach(async (record) => {

        if (!record.dynamodb.NewImage.StreamId)
            return;

        console.log(JSON.stringify(record));
        var eventSource = record.eventSourceARN;
        var tableName = eventSource.match(/:table\/([a-zA-Z0-9._\-]{3,255})\//)[1];

        var sequenceNumber = record.dynamodb.SequenceNumber;
        var dynamoKey = record.dynamodb.Keys;
        var approximateCreationDateTime = record.dynamodb.ApproximateCreationDateTime;
        var streamId = record.dynamodb.NewImage.StreamId.S;
        var key = record.dynamodb.NewImage.Key.S;
        var aggregateType = record.dynamodb.NewImage.AggregateType.S;
        console.log({
            'tableName': tableName,
            'sequenceNumber': sequenceNumber,
            'key': key,
            'streamId': streamId,
            'aggregateType': aggregateType,
            'approximateCreationDateTime': approximateCreationDateTime
        });

        try {
            await streamManager.createStream(streamId, key, aggregateType, approximateCreationDateTime, tableName);
            await sequenceNumberSyncronizer.syncronize(dynamoKey, sequenceNumber, approximateCreationDateTime, tableName);

            executedCount++;
            if (executedCount >= insertRecords.length) {
                callback(null, "ok");
            }
        } catch (err) {
            callback(err, null);
        }
    });
}