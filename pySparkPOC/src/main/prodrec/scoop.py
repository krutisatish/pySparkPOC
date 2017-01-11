import json
import sys
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark import SparkConf        # for configuring Spark
from pyspark import SparkContext     # to create a Spark Context


if __name__ == '__main__':
    # configure and get spark going
    conf = SparkConf().setMaster('yarn-cluster')
    conf = conf.setAppName('scoop')
    # give akka more time
    conf = conf.set("spark.core.connection.ack.wait.timeout","600")
    conf = conf.set("spark.shuffle.service.enabled", "false")
    # allocate memory split between shuffle and process memory
    conf = conf.set("spark.storage.memoryFraction", ".3")
    conf = conf.set("spark.shuffle.memoryFraction", ".7")
    conf = conf.set("spark.sql.codegen", "false")
    # elongate time outs so tasks don't fail
    conf = conf.set("spark.worker.timeout","30000")
    conf = conf.set("spark.akka.timeout","30000")
    conf = conf.set("spark.storage.blockManagerHeartBeatMs","300000")
    conf = conf.set("spark.storage.blockManagerSlaveTimeoutMs","900000")
    conf = conf.set("spark.akka.retry.wait","30000")
    conf = conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # create spark and SQL contexts (Hive)
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)
    #sqlContext = SQLContext(sc)
    sqlContext = HiveContext(sc)
    
    total = len(sys.argv)
    
    if (total != 2):
        
        print "Invalid number of arguments provided; Please provide correct number of arguments to the pyspark program"
        sys.exit();
        
    else:
        scoop_dtl = sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS scoop_dtl (precimaId string,divisionNumber string,customerNumber string,productNumber string,prod_desc string,prod_brnd string,pack_size string,qty_uom string,sales_opp string,prc_guidance string,pimMerchandisingCategoryDescription string,pimMerchandisingCategoryId string, productRank string) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' LOCATION 's3://aws.usf.data/usf_sfdc_scoop_rec_dtl/' TBLPROPERTIES ('serialization.null.format'='', 'skip.header.line.count'='2')")
        scoop_hdr = sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS scoop_hdr (precimaId string,description string,c string,startDate string,endDate string,divisionNumber string,customerNumber string) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' LOCATION 's3://aws.usf.data/usf_sfdc_scoop_rec_hdr/' TBLPROPERTIES ('serialization.null.format'='', 'skip.header.line.count'='2')")
              
        joinstmt = sqlContext.sql("SELECT CONCAT(lpad(a.divisionNumber,6,0),'_',lpad(a.customerNumber,10,0)) as identifier,a.productNumber,a.productRank,b.description,b.startDate,b.endDate,unix_timestamp(b.startDate,'MM/dd/yy') as startDateUnix, unix_timestamp(b.endDate,'MM/dd/yy') as endDateUnix FROM scoop_dtl a join scoop_hdr b on (a.divisionNumber =b.divisionNumber AND a.customerNumber =b.customerNumber)")
        
       
        schema = ("productNumber","startDate","productRank","description","endDate","startDateUnix", "endDateUnix")
        
        partsRDD = (joinstmt.map(lambda r: (r.identifier, (r.productNumber,r.startDate,r.productRank,r.description,r.endDate,r.startDateUnix,r.endDateUnix)))) 
        
        RDDkv = partsRDD.map(lambda (k,v) : (k, json.dumps(dict(zip(schema,v)))))
        
        scoopcmbn = RDDkv.combineByKey(lambda x: [(x)],
                                            lambda l,x: l + [(x)],
                                            lambda l1, l2: l1 + l2)
        
        
        RDDformatted = scoopcmbn.map(lambda (k,v) : k + "~" + "|".join(v))
        
        
        storeRDD = RDDformatted.saveAsTextFile(sys.argv[1])
        
        sc.stop()