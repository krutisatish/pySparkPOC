import json
import sys
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from pyspark import SparkConf        # for configuring Spark
from pyspark import SparkContext     # to create a Spark Context

if __name__ == '__main__':
    # configure and get spark going
    conf = SparkConf().setMaster('yarn-cluster')
    conf = conf.setAppName('reco_ranking')
    # give akka more time
    conf = conf.set("spark.core.connection.ack.wait.timeout","600")
    conf = conf.set("spark.shuffle.service.enabled", "false")
    # allocate memory split between shuffle and process memory
    conf = conf.set("spark.storage.memoryFraction", ".3")
    conf = conf.set("spark.shuffle.memoryFraction", ".7")
    conf = conf.set("spark.sql.shuffle.partitions", "500")
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
    sqlContext = SQLContext(sc)
    sqlContext = HiveContext(sc)
    
    total = len(sys.argv)
    
    if (total != 2):
        
        print "Invalid number of arguments provided; Please provide correct number of arguments to the pyspark program"
        sys.exit();
    else:
        
        reco_rank_tbl = sqlContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS reco_rank (precimaId string,divisionNumber string,customerNumber string,recommendationType string,rank string) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' LOCATION 's3://aws.usf.data/usf_sfdc_reco_ranking/' TBLPROPERTIES ('serialization.null.format'='', 'skip.header.line.count'='2')")    
        recData = sqlContext.sql("SELECT CONCAT(lpad(divisionNumber,6,0),'_',lpad(customerNumber,10,0)) AS identifier,recommendationType, rank FROM reco_rank WHERE lower(precimaId) != 'precima_id' and lower(precimaId) NOT LIKE '%usf_sfdc_reco_ranking%'")
            
        schema = ("recommendationType","rank")
            
        partsRDD = (recData.map(lambda r: (r.identifier, (r.recommendationType,unicode(int(r.rank)+1)))))
        
        RDDkv = partsRDD.map(lambda (k,v) : (k, json.dumps(dict(zip(schema,v)))))
            
        scoopcmbn = RDDkv.combineByKey(lambda x: [(x)],
                                                lambda l,x: l + [(x)],
                                                lambda l1, l2: l1 + l2)
        
        
        RDDformatted = scoopcmbn.map(lambda (k,v) : k + '~{"recommendationType": "My Kitchen", "rank": "1"}|' + "|".join(v)).repartition(300)
        
        
        
        storeRDD = RDDformatted.saveAsTextFile(sys.argv[1])
        
        
        sc.stop()


