
# Set Seldon configuration in zookeeper

We provide an example python script whcih requires kazoo : https://kazoo.readthedocs.org/en/latest/

Create a data file. An example is in example-client-zookeeper-config.txt. Each line has 3 values space separated:

`<list of clients comma separated> <zookeeper node> <data>`

$CLIENT will be replaced with each client name in turn if found. For non-client nodes just add a single initial value to first column, e.g. void

Example:

```
#
# database pool
#
void /config/dbcp {"dbs":[{"name":"ClientDB","jdbc":"jdbc:mysql://localhost:3306?characterEncoding=utf8","driverClassName":"com.mysql.jdbc.ReplicationDriver","user":"user1","password":"mypass"}]}
#
# Client dbcp name
#
client1 /all_clients/$CLIENT {"DB_JNDI_NAME":"ClientDB"}
#
#
#
client1 /all_clients/$CLIENT/algs {"algorithms":[{"name":"recentItemsRecommender","includers":[]}],"combiner":"firstSuccessfulCombiner"}
```

To set the example config assuming  you have a local zookeeper run:

```
cat example-client-zookeeper-config.txt | python set-client-config.py --zookeeper localhost 
```
