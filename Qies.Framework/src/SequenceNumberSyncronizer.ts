import * as AWS from 'aws-sdk';

export class SequenceNumberSyncronizer {
    constructor(private dynamodb: AWS.DynamoDB) {
    }

    public async syncronize(key: any, sequenceNumber: string, tableName: string) : Promise<void> {
        var params = {
            ExpressionAttributeNames: {
                "#SequenceNumber": "SequenceNumber",
                "#Created": "Created"
            },
            ExpressionAttributeValues: {
                ":SequenceNumber": {
                    S: sequenceNumber
                },
                ":Now": {
                    N: Date.now().toString()
                }
            },
            UpdateExpression: "SET #SequenceNumber = :SequenceNumber, #Created = :Now",
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