from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import sys, json

def integrate_resource_info(x):
      scheduler_info = dict()
      node_infos = dict()
      placement = x.get("placement")
      for index, value in enumerate(x.get("workNodeName")):
            node_info = dict()
            node_info["node_resource"] = str(x.get("workNodeResource")[index][0]) + ',' + str(x.get("workNodeResource")[index][1])
            vnf_in_node = list()
            for vnf_value in placement[index]:
                  vnf_info = x.get("vnfNameList")[vnf_value-1] + ',' + str(x.get("vnfRequestList")[vnf_value-1][0]) + ',' + str(x.get("vnfRequestList")[vnf_value-1][1])
                  vnf_in_node.append(vnf_info)
            node_info["vnf"] = vnf_in_node
            node_infos[value] = node_info
      return json.dumps(node_infos)

if __name__ == "__main__":
      sc = SparkContext("local[2]", appName="PythonStreamingKafkaWordCount")
      ssc = StreamingContext(sc, 5)  #每5秒偵測一次

      kvs = KafkaUtils.createStream(ssc, "10.0.0.131:2181", "spark-streaming-consumer", {"quickstart-events": 1})
      lines = kvs.map(lambda x: json.loads(x[1]))\
            .map(integrate_resource_info)
      lines.pprint()
      ssc.start()
      ssc.awaitTermination()