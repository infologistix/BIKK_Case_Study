import numpy as np
import pandas as pd
from confluent_kafka import Producer
from connect import get_connection
from random import randint
from simulation import transaktion_factory
from time import sleep


"""
    START_NUM_CUST : Ab dem wie vielten Kunden kommen neue dazu. Wie viele sind schon da?
    NUM_SIMUL : Anzahl der simulierten Events
    SEP : Separator in der Kafka Message
    Version : "all" -> alle Bewertungen mit und ohne Rezensionen [default]
              "R"   -> nur Rezensionen ohne Bewertung
              "RB"  -> Rezensionen mit Bewertung
    INTERVALL : Zeitintervall zwischen den Nachrichten
    CONF : Configuration for the Producer
"""
START_NUM_CUST = 250
NUM_SIMUL = 20000
SEP = "; "
VERSION = "RB"
INTERVALL = (0, 0.03)

CONF = {'bootstrap.servers': 'broker:19092'}
# CONF = {'bootstrap.servers': '0.0.0.0:9092'}


def bew_version(version=VERSION):
    if VERSION == "RB":
        return pd.read_sql("Rezensionen", con=engine)
    elif VERSION == "R":
        rez = pd.read_sql("Rezensionen", con=engine)
        rez.loc[:,"Wertung"] = None
        return rez
    else:  
        return pd.read_sql("Bewertung", con=engine)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def produce_message(topic: str, 
                    kind: str, 
                    index: int, 
                    dframe: pd.DataFrame, 
                    customer: int = None) -> str:
    data = SEP.join([str(x) for x in dframe.iloc[index, :]])
    kind = kind + SEP + str(customer) + SEP if customer else kind + SEP
    data = kind + data
    p.produce(topic, key=str(index), value=data, callback=delivery_report)
    p.poll(0)
    # sleep(randint(*INTERVALL))
    sleep(0.001)
    return data

def produce_ratio(topic: str, ratio) -> str:
    data = str(ratio)
    p.produce(topic, value=data, callback=delivery_report)
    p.poll(0)
    return data


engine = get_connection()
with engine.connect() as connection:
    df = pd.read_sql("Kunde", con=engine)
    bs = pd.read_sql("KafkaBestand", con=engine)
    bs = bs[['ID_Bestand', 'Titel', 'Autoren', 'Jahr', 'Gattung', 'Kennung', 'Zugriffsort']]
    bew = bew_version(VERSION)

p = Producer(CONF)

topic_choices = ["Neukunden", "Transaktion", "Bewertung"]
topics = np.random.choice(topic_choices, NUM_SIMUL, p=[0.05, 0.85, 0.1])

action = transaktion_factory(START_NUM_CUST, bs.shape[0])
counter = 0

try:
    for topic in topics:
        if topic=="Transaktion":
            counter += 1
            if counter < 2:
                buch, cust = action.leihe_buch()
                produce_message(topic, "Leihe", buch, bs, cust)
            elif counter%100 == 0:
                p.flush()
            else:
                state = set(action.state)
                ratio = 1. - len(state) / len(range(1,action.b_count))
                ratio *= 0.7
                produce_ratio(topic, ratio)
                würfel = np.random.rand()
                if würfel < ratio:
                    buch, cust = action.leihe_buch()
                    produce_message(topic, "Leihe", buch, bs, cust)
                else:
                    buch, cust = action.rückgabe_buch()
                    produce_message(topic, "Rückgabe", buch, bs, cust)

        elif topic=="Neukunden":
            action.add_customer()
            produce_message(topic, "Neukunde", action.c_count, df)
        else:
            c_bew = randint(1, bew.shape[0] - 1)
            msg = produce_message(topic, "Bewertung", c_bew, bew)
            
except KeyboardInterrupt:
    p.flush()

