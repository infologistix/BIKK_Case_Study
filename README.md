# Kafka Anleitung
*vom 21. September 2023*

Das ist eine kleine Doku für mich und vielleicht für andere, die immer wieder vergessen wie die Kafka Befehle lauten
und was die ganzen yml files und die ganzen Container machen.


## Single Zookeeper, single Kafka

Probiere zunächst Kafka aus. Die schlankeste Version ist die mit nur einem Zookeeper und einem Kafka.
Diesen Verbund kann man ausführen, herunterfahren über:

````
docker compose -f zk-single-kafka-single.yml up
docker compose -f zk-single-kafka-single.yml down
````

Das erzeugt zwei Container mit den Namen *zoo1* und *kafka1*. Jetzt brauchst du zwei Terminals, eins für den Producer und eins für den Consumer. Zunächst der Producer:

````
docker exec -it kafka1 bash
kafka-console-producer --bootstrap-server kafka1:9092 --topic test
kafka-console-producer --bootstrap-server kafka1:9092 --topic test --property parse.key=true --property key.separator=,
````

Bei der zweiten Version kann man einen Key hinzufügen. Schreibe nun beliebig viele Nachrichten in dein Topic namens *test* hinein.

Ähnlich ist es beim Consumer (im zweiten Terminal, um zu sehen ob die Daten sofort ankommen):

````
docker exec -it kafka1 bash
kafka-console-consumer --bootstrap-server kafka1:9092 --from-beginning --topic test
kafka-console-consumer --bootstrap-server kafka1:9092 --from-beginning --topic test --property print.key=true
````

## Full Stack Kafka

Möglicherweise braucht man mehr um Kafka zu nutzen. Für die volle Bandbreite sind in *docker-compose.yml* folgende weitere container

- schema registry
- rest-proxy-server
- ksql
- kafka connect
- MySQL 5.7

Die Versionen sind nach jetzigem Stand alle 7.3.2. Man führt es genauso aus wie oben beschrieben. Wenn irgendwas nicht benötigt wird, dann kann man das einfach auskommentieren
um Resourcen zu sparen.

Hier in diesem Beispiel ist noch zusätzlich ein python Container mit einem Producer gegeben.
Das Image muss man zunächst bauen, bevor der Container mit den anderen Containern zusammen mit docker compose up -d hochfahren kann.
Wenn man im Kafka Ordner ist also mit:
    
    docker build -t producer ./producer/
    docker build -t consumer ./consumer/

da dort das zugehörige Dockerfile liegt. Dann einfach mit 

    docker compose up -d

starten.

## Kafka mit Conduktor

Es gibt zwei Control-center die man verwenden kann. Das *cp-enterprise-control-center* hat bei mir mit der Version 7.3.2. nicht funktioniert.

Conduktor ist eine weitere Alternative. Man muss sich auf der Website registrieren. Das Tool ist aber komplett kostenlos. 

Bei mir hat nur die Desktop Variante funktioniert weiter unten. Die Docker Variante wäre
folgenden Befehl auszuführen.


````
curl -L https://releases.conduktor.io/console -o docker-compose.yml && docker compose up
````

Und im Browser auf *localhost:8080* lauschen.

Falls das nicht klappt dann gibt es die Möglichkeit Conduktor Desktop zu verwenden und sich
dann mit seinem cluster zu verbinden. Das geht sehr einfach. Das kann man hier herunterladen:

````
https://www.conduktor.io/get-started/#desktop
````

Da nach außen der Port 9092 freigegeben ist, verbindet man sich einfach mit 

    localhost:9092


## Connect MySQL to MySQL Workbench

Um MySQL zu Workbench (oder python, oder DBVisuazlizer, etc) zu verbinden, nehme

````
Hostname: 127.0.0.1
port: 3307
Username: root
Password: root
````

### Error Handling

Sollte das nicht funktionieren, dann checke, ob der Root für alle Hosts freigeschaltet ist.

````
docker exec -it mysql57 bash
mysql -uroot -p
````

Mit folgendem Befehl kann man sich alle user und hosts anschauen.

````
select host, user from mysql.user;
````

Bei root sollte % stehen. Steht da nur localhost, dann schalte den root 
frei mit 

````
update mysql.user set host = '%' where user='root';
````

Gehe mit exit aus dem Container raus und starte ihn neu mit 

````
docker restart mysql57
````

Starte nun MySQL Workbench und erstelle eine neue Connection.


Im Verbund wird automatisch eine Datenbank mit dem Namen 'bibdb' erstellt. 

Zum Testen kann man schauen, ob alle Tabellen da sind.

````
use bibdb;
show tables;
````

## python + mysql

Um eine Verbindung in python zu mysql herzustellen. Kann man zum Beispiel SQLAlchemy verwenden. Das funktioniert auch gut in der Kombi mit pandas.

``` python
from sqlalchemy import create_engine, text

USER = 'root'
PASSWORD = 'root'
HOST = 'mysql57'          # see yml file
PORT = 3306
DATABASE = 'bibdb'
 

def get_connection(database=DATABASE):
    return create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}"\
            .format(USER, PASSWORD, HOST, PORT, database)
    )
```

Das stellt die Verbindung zur Datenbank her. Aber was genau unterscheidet SQLAlchemy von
anderen möglichen SQL-Bibliotheken? Hier eine Beschreibung:

    What is SQLAlchemy ORM ? SQLAlchemy ORM (Object Relational Mapper) is a higher-level API built on top of SQLAlchemy Core, providing an easier way to interact with databases using Python classes and objects. It allows you to map Python classes to database tables, and to interact with the data in those tables using instances of those classes.

Man kann also sehr einfach SQL Befehle in "python-Sprache" ausführen. Siehe dafür hier:

    https://docs.sqlalchemy.org/en/20/orm/quickstart.html

Aber wenn man darauf keine Lust oder keine Zeit und "echte SQL-Statements" verwenden möchte, kann man das gerne tun. Zum Beispiel so:

```python
engine = get_connection()

with engine.connect() as connection:
    ### Hier ist eine Beispiel SQL
    sql = text("Show tables;")
    result = connection.execute(sql)
    print("Befehl wird ausgeführt, aber man sieht nicht viel", result)
    # Das erzeugt nämlich einen Generator. Um den Output zu sehen muss man iterieren.
    for row in result:
        print(row)
```

## Starting the Producer

Um den producer zu starten, muss dieser in der console des python containers gestartet werden:

````
app# python3 producer.py
````
