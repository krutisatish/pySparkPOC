
'''code to be executed from hive
    
    CREATE EXTERNAL TABLE IF NOT EXISTS last4buy (div_nbr string,customer_number string,dept_number string,product_number string,last_purchase_date date,last_purchase_price string) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' LOCATION 's3://aws.usf.qa.data/lastbuy/' TBLPROPERTIES ('serialization.null.format'='');    
    
    CREATE EXTERNAL TABLE IF NOT EXISTS last4buy_dynamo (identifier string, purchaseDate string) STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' TBLPROPERTIES ('dynamodb.table.name' = 'productrecos_lastbuy_qa', 'dynamodb.column.mapping' ='identifier:identifier,purchaseDate:purchaseDate');    
    
    INSERT OVERWRITE TABLE last4buy_dynamo SELECT CONCAT(lpad(lb.div_nbr,6,0),'_',lpad(lb.customer_number,10,0),'_',lpad(lb.product_number,10,0)) AS identifier, from_unixtime(unix_timestamp(lb.last_purchase_date,'yyyyMMdd'),'MM-dd-yyyy') AS purchaseDate FROM (select div_nbr,customer_number,product_number,last_purchase_date FROM (select  div_nbr,customer_number,product_number,last_purchase_date,row_number() OVER  (PARTITION BY div_nbr,customer_number,product_number order by  from_unixtime(unix_timestamp(last_purchase_date,'yyyyMMdd'),'MM-dd-yyyy') DESC ) as idx from last4buy ) rel where idx =1) lb;
'''
