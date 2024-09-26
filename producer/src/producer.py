import numpy as np
import pandas as pd
from confluent_kafka import Producer
from connect import get_connection
from random import randint
from simulation import transaktion_factory
from time import sleep
from datetime import datetime, timedelta


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
INTERVALL = (0, 3)
START_TIME = "24/09/28"
START_DATE = datetime.strptime(START_TIME, "%y/%m/%d")

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
    sleep(randint(*INTERVALL))
    # sleep(0.001)
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
counter = 0.
coustomer_count = 0
buch_date_dict = {}
date = START_DATE

try:
    for topic in topics:
        
        if topic=="Transaktion":
            würfel = np.random.rand()
            Fernleihe = "True"
            if würfel < 0.7:
                Fernleihe = "False"
            counter += 1.
            delta = würfel * 6
            date += timedelta(hours=int(delta))
            #produce_ratio(topic, str(delta))
            #produce_ratio(topic, str(date))
            if counter < 6:
                buch, cust = action.leihe_buch()
                produce_message(topic, "Leihe; " + str(date) + "; " + Fernleihe, buch, bs, cust)
                # produce_message(topic, "Leihe", buch, bs, cust, Ausleihdatum, Rückgabedatum)
            elif counter%100 == 0:
                p.flush()
            else:
                state = set(action.state)
                ratio = 1. - len(state) / action.b_count
                ratio *= 0.7
                # produce_ratio(topic, ratio)
                if würfel < ratio:
                    buch, cust = action.leihe_buch()
                    produce_message(topic, "Leihe; " + str(date) + "; " + Fernleihe, buch, bs, cust)
                else:
                    buch, cust = action.rückgabe_buch()
                    produce_message(topic, "Rückgabe; " + str(date) + "; None", buch, bs, cust,)

        elif topic=="Neukunden":
            action.add_customer()
            produce_message(topic, "Neukunde; " + str(coustomer_count), action.c_count, df)
            coustomer_count += 1
        else:
            c_bew = randint(1, bew.shape[0] - 1)
            msg = produce_message(topic, "Bewertung", c_bew, bew)
            
except KeyboardInterrupt:
    p.flush()
