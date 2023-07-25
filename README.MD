# Kafka Connect

1. Télécharger les images et créer les containers:

docker-compose up -d

2. Attendre le démarrage de Kafka Connect

bash -c ' \
echo -e "\n\n=============\nWaiting for Kafka Connect to start listening on localhost ⏳\n=============\n"
while [ $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) -ne 200 ] ; do
  echo -e "\t" $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) " (waiting for 200)"
  sleep 5
done
echo -e $(date) "\n\n--------------\n\o/ Kafka Connect is ready! Listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) "\n--------------\n"
'

3. Vérifier que les connecteurs Elasticsearch et Debezium sont chargés

curl -s localhost:8083/connector-plugins|jq '.[].class'|egrep 'MySqlConnector|ElasticsearchSinkConnector'

Le retour doit être:

"io.confluent.connect.elasticsearch.ElasticsearchSinkConnector"
"io.debezium.connector.mysql.MySqlConnector"

4. Lancer le CLI mysql

docker exec -it mysql bash -c 'mysql -u root -p$MYSQL_ROOT_PASSWORD demo'

5. Une base de données MySQL stocke les commandes d'une application e-Commerce.

Le connecteur Kafka Connect - Debezium MySQL stream les commandes vers un topic Kafka
Le connecteur Kafka Connect - Elasticsearch stream les commandes d'un topic Kafka vers Elasticsearch


6. Vérifier les données de la table ORDERS

SELECT * FROM ORDERS ORDER BY CREATE_TS DESC LIMIT 1\G

7. Injecter les données dans la table ORDERS

docker exec mysql /data/02_populate_more_orders.sh

8. Vérifier les nouveaux enregistrements

watch -n 1 -x docker exec -t mysql bash -c 'echo "SELECT * FROM ORDERS ORDER BY CREATE_TS DESC LIMIT 1 \G" | mysql -u root -p$MYSQL_ROOT_PASSWORD demo'

9. Créer le connecteur MySQL

curl -i -X PUT -H  "Content-Type:application/json" \
    http://localhost:8083/connectors/source-debezium-orders-00/config \
    -d '{
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "mysql",
            "database.port": "3306",
            "database.user": "debezium",
            "database.password": "dbz",
            "database.server.id": "42",
            "database.server.name": "asgard",
            "table.whitelist": "demo.orders",
            "database.history.kafka.bootstrap.servers": "broker:29092",
            "database.history.kafka.topic": "dbhistory.demo" ,
            "decimal.handling.mode": "double",
            "include.schema.changes": "true",
            "transforms": "unwrap,addTopicPrefix",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.addTopicPrefix.type":"org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.addTopicPrefix.regex":"(.*)",
            "transforms.addTopicPrefix.replacement":"mysql-debezium-$1"
    }'

10. Vérifier le statut du connecteur

curl -s "http://localhost:8083/connectors?expand=info&expand=status" | \
       jq '. | to_entries[] | [ .value.info.type, .key, .value.status.connector.state,.value.status.tasks[].state,.value.info.config."connector.class"]|join(":|:")' | \
       column -s : -t| sed 's/\"//g'| sort

Le résultat doit être 

source  |  source-debezium-orders-00  |  RUNNING  |  RUNNING  |  io.debezium.connector.mysql.MySqlConnector

11. Vérifier le topic

docker exec kafkacat kcat \
        -b broker:29092 \
        -r http://schema-registry:8081 \
        -s value=avro \
        -t mysql-debezium-asgard.demo.ORDERS \
        -C -o -10 -q | jq '.id, .CREATE_TS.string'

12. Streamer les données depuis Kafka vers Elasticsearch

curl -i -X PUT -H  "Content-Type:application/json" \
    http://localhost:8083/connectors/sink-elastic-orders-00/config \
    -d '{
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "topics": "mysql-debezium-asgard.demo.ORDERS",
        "connection.url": "http://elasticsearch:9200",
        "type.name": "type.name=kafkaconnect",
        "key.ignore": "true",
        "schema.ignore": "true"
    }'

13. Vérifier les données depuis Elasticsearch

curl -s http://localhost:9200/mysql-debezium-asgard.demo.orders/_search \
    -H 'content-type: application/json' \
    -d '{ "size": 5, "sort": [ { "CREATE_TS": { "order": "desc" } } ] }' |\
    jq '.hits.hits[]._source | .id, .CREATE_TS'

14. Vérifier les données depuis Kibana

http://localhost:5601