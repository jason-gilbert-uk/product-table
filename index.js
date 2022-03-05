const AWS = require('aws-sdk')
AWS.config.update({region:'eu-west-1'});
const docClient = new AWS.DynamoDB.DocumentClient();

//----------------------------------------------------------------------------
// function: checkProductTableExists(tableName)
// returns true if table name exists, false otherwise.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function checkProductTableExists(tableName) {
    var status=false
    var params = { TableName: tableName };
    try {
        var dynamodb = new AWS.DynamoDB();
        var result = await dynamodb.describeTable(params).promise();
        status = true;
        return status;
    } catch (err) {
        if (err.code !== 'ResourceNotFoundException') {
            console.log(err, err.stack); // an error occurred
            throw err;
        }
        return status
    }
}

//----------------------------------------------------------------------------
// function: createProductTableIfDoesntExist(tableName)
// Checks to see if database exists, and creates it if not.
// Returns true if database exists, false otherwise.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function createProductTableIfDoesntExist(tableName) {
    if (await checkProductTableExists(tableName)) {
        return false;
    } else {
        var result = await createProductTable(tableName);
        return true;
    }
}

//----------------------------------------------------------------------------
// function: createProductTable(tableName)
// creates the database. Returns true if successful.
// Note: This function is very app specific.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function createProductTable (tableName) {
    var dynamodb = new AWS.DynamoDB();
 
    var params = {
        TableName : tableName,
        KeySchema: [       
            { AttributeName: "productId", KeyType: "HASH"}
        ],
        AttributeDefinitions: [       
            { AttributeName: "productId", AttributeType: "S" },
            { AttributeName: "discount", AttributeType: "N" }  
        ],
        ProvisionedThroughput: {       
            ReadCapacityUnits: 5, 
            WriteCapacityUnits: 5
           },
        GlobalSecondaryIndexes: [{
                IndexName: "discountIndex",
                KeySchema: [
                    {
                        AttributeName: "discount",
                        KeyType: "HASH"
                    }
                ],
                Projection: {
                    ProjectionType: "ALL"
                },
                ProvisionedThroughput: {
                    ReadCapacityUnits: 1,
                    WriteCapacityUnits: 1
                }
            }]
    };
 
    try {
        var result = await dynamodb.createTable(params).promise(); 
        return true
    } catch (err) {
        console.log('product-table.createProductTable encountered error : ',err);
        throw err;
    }
    
};

//----------------------------------------------------------------------------
// function: writeItemToProductTable(tableName,item)
// Writes the item to the table.
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function writeItemToProductTable(dbTableName,item) {

    var params = {TableName: dbTableName, Item: item }

    try {
        const result = await docClient.put(params).promise();
        return result;      
    } catch (err) {
        console.log('error in product-table.writeItemToProductTable: ',err)
        throw err;
    }
}

//----------------------------------------------------------------------------
// function: deleteProductTable(tableName)
// Deletes the named table. Returns true if succeeds, false if table doesnt 
// exist
// Throws exception on unexpected errors.
//----------------------------------------------------------------------------
async function deleteProductTable(dbTableName) {
    var dynamodb = new AWS.DynamoDB();
    var params = {
        TableName: dbTableName
    }

    try {
        var result = await dynamodb.deleteTable(params).promise();
        return true;
    } catch (err) {
        if (err.code !== 'ResourceNotFoundException') {
            console.log('error in productTabke.deleteProductTable : ',err);
            throw err;
        }
        return false;
    }
}

exports.createProductTableIfDoesntExist = createProductTableIfDoesntExist;
exports.createProductTable = createProductTable;
exports.writeItemToProductTable = writeItemToProductTable;
exports.deleteProductTable = deleteProductTable;
