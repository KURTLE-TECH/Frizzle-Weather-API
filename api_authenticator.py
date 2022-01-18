import boto3
import random 
import string
import database
from database.DynamodbHandler import DynamodbHandler
from json import dumps
from datetime import datetime
import logging
import base64
import hashlib

class ApiAuthenticator:
    def __init__(self,config):               
        self.table = config['api_table']                
        self.handler = DynamodbHandler(region="ap-south-1")                 

    def generate_key(self): 
        try:
            key = ''.join(random.choices(string.ascii_lowercase + string.digits, k = 20))                        
            hashed_key = hashlib.md5(key.encode('utf-8'))
            

        except Exception as e:
            logging.error(e.__str__,e.__traceback__.tb_lineno)
            return {"status":"failed","reason":e.__str__}

        try:
            time_stamp = datetime.now().strftime("%Y-%M-%D_%H:%M:%S")            
            table_values = {"key":hashed_key.hexdigest(),"generated_time":time_stamp,"is_active":"true"}
            result = self.handler.insert(table_values,self.table)                                    
        except Exception as e:
            logging.error(e.__str__)
            return {"status":"failed","reason":e.__str__}
        return key
        


    def validate_key(self,key):  
        try:
            encrypted_key_string = hashlib.md5(key.encode('utf-8'))        
        except Exception as e:
            return False
        result = self.handler.query(self.table,"key",encrypted_key_string.hexdigest())        
        if result['status']=="success":            
            if result['Response']['is_active'] =='false':
                return False
            return True
        return False    
        
        
    def update_key(self,key,condition):
        encrypted_key_string = hashlib.md5(key.encode('utf-8'))        
        result = self.handler.query(self.table,"key",encrypted_key_string.hexdigest())
        if result['status']=="success":
            new_value = result['Response']
            new_value['is_active'] = condition
            try:
                result = self.handler.insert(new_value,self.table)                                    
            except Exception as e:
                logging.error(e.__str__)
                return {"status":"failed","reason":e.__str__}
            
            logging.info("Updated key")            
            return {"status":"success"}
        return {"status":"failed","reason":"key does not exist in database"}


# key = auth_object.generate_key()
# print("This is key",key)
# key = auth_object.validate_key("e2oxq0jv4uel7k2pxgfv")
# key = auth_object.update_key("e2oxq0jv4uel7k2pxgfv",'false')
# print(key)