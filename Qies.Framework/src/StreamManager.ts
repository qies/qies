export class StreamMananger {
    private createdStreams = new Array<String>();

    constructor(private dynamodb: AWS.DynamoDB) {
        console.log("Stream manager created");
    }

    public async createStream(streamId: string, key: string, aggregateType: string, tableName: string): Promise<void> {
        console.log(JSON.stringify(this.createdStreams));
        if (this.createdStreams.indexOf(streamId) >= 0)
            return;

        var params = {
            ExpressionAttributeNames: {
                "#Id": "Id",
                "#Key": "Key",
                "#Created": "Created",
                "#AggregateType": "AggregateType"
            },
            ExpressionAttributeValues: {
                ":Key": {
                    S: key
                },
                ":Created": {
                    N: Date.now().toString()
                },
                ":AggregateType": {
                    S: aggregateType
                }
            },
            UpdateExpression: "SET #Key = :Key, #Created = :Created, #AggregateType = :AggregateType",
            Key: {
                "Id": {
                    S: streamId
                }
            },
            TableName: tableName,
            ReturnValues: 'NONE',
            ConditionExpression: "attribute_not_exists(#Id)"
        } as AWS.DynamoDB.UpdateItemInput;
        try {
            await this.dynamodb.updateItem(params).promise();
            this.createdStreams.push(streamId);
        } catch (err) {
            if (err.code === "ConditionalCheckFailedException")
                return;
            console.log(err);
            throw err;
        }
    }
}