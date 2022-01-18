import boto3

class DynamodbHandler:
    def __init__(self,region):
        self.db = boto3.resource('dynamodb',region_name=region)
        self.client = boto3.client('dynamodb',region_name=region)
    
    def test(self):
        return "Yes its working"
    
    def insert(self,node_sensor_values,table):
        try:
            table = self.db.Table(table)
            row = {i:node_sensor_values[i] for i in node_sensor_values.keys()}
            with table.batch_writer() as writer:
                writer.put_item(Item=row)
            return {"Status":"successful"}
        except Exception as e:
            return {"Status":"failed", "reason":e}
    
    def query(self,table_name,partition_key,partition_key_value):
        try:
            table = self.db.Table(table_name)
            response = table.get_item(
                                    Key={
                                        partition_key:partition_key_value
                                    },                            
                                    ConsistentRead=True,
                                    )

            if 'Item' not in response.keys():                
                return {"status":"failed","reason":f"item does not exist in table {table_name}"}
            return {"status":"success","Response":response["Item"]}

        except Exception as e:
           return {'status':'failed','reason':str(e)}



    def view_database(self,table):
        try:
            response = self.client.scan(
                TableName = table,
            )
            return response
        except Exception as e:
            return {"Status":"Failed","Reason":e}
    def create_table_in_database(self,table_name):
        try:
            response = self.client.create_table(
                AttributeDefinitions=[
                {
                'AttributeName': 'time-stamp',
                'AttributeType': 'S'
                },
                ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'time-stamp',
                    'KeyType': 'HASH'
                },
            ],
            BillingMode='PAY_PER_REQUEST',
            StreamSpecification={
                'StreamEnabled': False,
            },
            SSESpecification={
                'Enabled': True ,
                'SSEType': 'KMS',
                'KMSMasterKeyId': '57780289-75ee-4f41-bdf8-0d4f43291fae'
            }
            )
            return response
        except Exception as e:
            print(e)
    # def display_values(self):



