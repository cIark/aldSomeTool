package hbase

object HBaseUtil {
  import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
  import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}


  object HBaseUtil {
    val configuration = HBaseConfiguration.create()
    var connection = Connection
    var admin = Admin


    def main(args: Array[String]): Unit = {

    }

    def getHbaseConn(zkUrl: String, zkPort: String) = {
      val configuration = HBaseConfiguration.create

      configuration.set("hbase.zookeeper.quorum", zkUrl)

      configuration.set("hbase.zookeeper.property.clientPort", zkPort)
      val connection = ConnectionFactory.createConnection(configuration)

      connection
    }


    def creatTable(admin: Admin, tableName: String, colFamily: Array[String]): Boolean = {
      if (admin.tableExists(TableName.valueOf(tableName))) {
        false
      } else {
        val descriptor = new HTableDescriptor(TableName.valueOf(tableName))
        colFamily.foreach(col => {
          descriptor.addFamily(new HColumnDescriptor(col))
        })
        admin.createTable(descriptor)
        true
      }



    }

  }

}
