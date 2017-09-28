import * as AWS from 'aws-sdk';

export class SequenceNumberSyncronizer {
    constructor(private dynamodb: AWS.DynamoDB) {
    }

    public async syncronize(key: any, sequenceNumber: string, createdTime: number, tableName: string) : Promise<void> {
        var params = {
            ExpressionAttributeNames: {
                "#SequenceNumber": "SequenceNumber",
                "#Created": "Created",
                "#EntryType": "EntryType"
            },
            ExpressionAttributeValues: {
                ":SequenceNumber": {
                    S: sequenceNumber
                },
                ":CreateTime": {
                    N: createdTime.toString()
                },
                ":EntryType": {
                    S: "Event"
                }
            },
            UpdateExpression: "SET #SequenceNumber = :SequenceNumber, #Created = :CreateTime, #EntryType = :EntryType",
            Key: key,
            TableName: tableName,
            ReturnValues: 'NONE'
        } as AWS.DynamoDB.UpdateItemInput;
        try {
            console.log(params);
            await this.dynamodb.updateItem(params).promise();

        } catch (err) {
            console.log(err);           
        }
    }
}