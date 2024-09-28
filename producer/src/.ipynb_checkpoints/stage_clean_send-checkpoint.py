

import numpy as np
import pandas as pd
from confluent_kafka import Producer
from connect import get_connection
from random import randint
from simulation import transaktion_factory
from time import sleep
import socket
from mysql.connector import connect

from confluent_kafka import Consumer

    
from sqlalchemy import create_engine, inspect, text

USER = 'root'
PASSWORD = 'root'
HOST = 'mysql57' # 'localhost'          # see yml file
PORT = 3306 # 3307
DATABASE = 'test_db_1'
     

query_drop_old_DWH_database= """
drop database if exists test_db_1;
drop database if exists M_Leihe;
drop database if exists M_Verfuegbarkeit;
drop database if exists M_Bewertung;
"""

mysql_connection = connect(user=USER, password=PASSWORD, host=HOST, port=PORT)#, database=DATABASE)
cursor = mysql_connection.cursor()
cursor.execute(query_drop_old_DWH_database, multi=True)
cursor.close() 



# # Einlesen des Buchbestands


df_bestand = pd.read_csv('data/Bestand.csv', sep=';', encoding='unicode_escape')



bestand_cols=df_bestand.columns






# # Einlesen der Tabellen des Producers




c = Consumer({
    'bootstrap.servers': 'broker:19092',
    'group.id': socket.gethostname(),
    'auto.offset.reset': 'latest'
})


prod_Transaktion_cols={
    0:'Aktion',
    1:'Datum',
    2:'Fernleihe',
    3:'ID_Kunde',
    4:'ID_Exemplar',
    5:'Titel',
    6:'Autor',
    7:'Jahr',
    8:'Art',
    9:'Kennung',
   10:'Zugriffsort'
}

prod_Bewertung_cols={
    0:'Aktion',
    1:'ID_Kunde',
    2:'ID_Buch',
    3:'Wertung',
    4:'Rezension'
}

prod_Neukunden_cols={
    0:'Aktion',
    1:'ID_Kunde',
    2:'c_count',
    3:'Kundennr',
    4:'Vorname_1',
    5:'Vorname_2',
    6:'Nachname',
    7:'Anrede',
    8:'PLZ',
    9:'Strasse',
   10:'Hausnr',
   11:'Mail',
   12:'Tel',
   13:'Geschlecht',
   14:'Geburtsdatum',
   15:'Beruf',
   16:'Titel',
   17:'PersoNr',
   18:'PersoValidTo' 
}

cols=[prod_Transaktion_cols,prod_Bewertung_cols,prod_Neukunden_cols]
topics=["Transaktion", "Bewertung", "Neukunden"]



bestandskunden_flag = 1
checkcount=0
statische_tabellen_flag = 1
while 1:
    checkcount+=1
    
    dfd={}
    
    # # Konsumiere Daten
        
    for col, topic in zip(cols, topics):
        print(col, topic)
    
        dfd[topic] = pd.DataFrame(columns=list(col.values()))
        
        
        c.subscribe([topic])
        
        for i in range(10):
            msg = c.poll()
            if msg is None:
                continue
        
            mrow = pd.DataFrame([msg.value().decode('utf-8').split(";")]).rename(columns=col)
    
            dfd[topic]=pd.concat([dfd[topic], mrow])
    
        dfd[topic] = dfd[topic].reset_index(drop=True)
        #c.close()
        
    
    
    
    
    # # Cleaning der Tabellen
    
    # ### Transaktion
    
    if not dfd['Transaktion'].empty:

        # IDs zu int konvertieren
        dfd['Transaktion']['ID_Exemplar'] = pd.to_numeric(dfd['Transaktion']['ID_Exemplar']).astype('Int64', errors='ignore')
        dfd['Transaktion']['ID_Kunde'] = pd.to_numeric(dfd['Transaktion']['ID_Kunde']).astype('Int64', errors='ignore')
        # Jahre zu int konvertieren
        dfd['Transaktion']['Jahr'] = np.where(dfd['Transaktion']['Jahr'].astype(str).str.contains("nan"), np.nan, dfd['Transaktion']['Jahr'] )
        dfd['Transaktion']['Jahr'] = pd.to_numeric(dfd['Transaktion']['Jahr']).astype('Int64', errors='ignore')
        
        # Fernleihe zu true oder false konvertieren
        dfd['Transaktion']['Fernleihe'] = np.where(dfd['Transaktion']['Fernleihe'].astype(str).str.contains("None"), False, dfd['Transaktion']['Fernleihe'] )
        dfd['Transaktion']['Fernleihe'] = np.where(dfd['Transaktion']['Fernleihe'].astype(str).str.contains("False"), False, dfd['Transaktion']['Fernleihe'] )
        dfd['Transaktion']['Fernleihe'] = np.where(dfd['Transaktion']['Fernleihe'].astype(str).str.contains("True"), True, dfd['Transaktion']['Fernleihe'] )

    # ### Bewertung
    
    if not dfd['Bewertung'].empty:
        
        # IDs zu int konvertieren
        dfd['Bewertung']['ID_Kunde'] = pd.to_numeric(dfd['Bewertung']['ID_Kunde']).astype('Int64', errors='ignore')
        dfd['Bewertung']['ID_Buch'] = pd.to_numeric(dfd['Bewertung']['ID_Buch']).astype('Int64', errors='ignore')
        dfd['Bewertung']['Wertung'] = pd.to_numeric(dfd['Bewertung']['Wertung']).astype('Int64', errors='ignore')
        
        
    # ### Neukunden
    if not dfd['Neukunden'].empty:
        
        # IDs zu int konvertieren
        dfd['Neukunden']['ID_Kunde'] = pd.to_numeric(dfd['Neukunden']['ID_Kunde']).astype('Int64', errors='ignore')
        dfd['Neukunden']['Kundennr'] = pd.to_numeric(dfd['Neukunden']['Kundennr']).astype('Int64', errors='ignore')
        
        # Anreden konsistent machen 
        if dfd['Neukunden']['Anrede'].str.contains('Fr\.', regex=True).any()==True:
            dfd['Neukunden']['Anrede'] = dfd['Neukunden']['Anrede'].replace(['Fr\.'],'Frau', regex=True)
        if dfd['Neukunden']['Anrede'].str.contains('Hr\.', regex=True).any()==True:
            dfd['Neukunden']['Anrede'] = dfd['Neukunden']['Anrede'].replace(['Hr\.'],'Herr', regex=True)
        # Titel: "None" to NaN
        dfd['Neukunden']['Titel']  = np.where(dfd['Neukunden']['Titel'].str.contains("None"), np.nan, dfd['Neukunden']['Titel'] )
        # Vorname_2: "nan" to NaN
        dfd['Neukunden']['Vorname_2']  = np.where(dfd['Neukunden']['Vorname_2'].str.contains("nan"), np.nan, dfd['Neukunden']['Vorname_2'] )
        # Hausnr: "nan" to NaN
        dfd['Neukunden']['Hausnr'] = dfd['Neukunden']['Hausnr'].astype(str)
        dfd['Neukunden']['Hausnr']  = np.where(dfd['Neukunden']['Hausnr'].str.contains("nan"), np.nan, dfd['Neukunden']['Hausnr'] )
        # 4-stellige PLZ mit "0" präfixen 
        dfd['Neukunden']['PLZ']    = dfd['Neukunden']['PLZ'].str.strip().str.rjust(5, '0')
        
        # Hausnummern aus Strasse-Spalte extrahieren
        def extract_hausnr(strasse):
          if pd.isna(strasse):
            return np.nan
          parts = strasse.split()
          if len(parts) > 1 and parts[-1].isdigit():
            return parts[-1]
          else:
            return np.nan
        #Apply extract_hausnr, wenn 'Hausnr' NaN und entferne die Hausnummern aus der Strasse Spalte
        dfd['Neukunden'].loc[dfd['Neukunden']['Hausnr'].isnull(), 'Hausnr'] = dfd['Neukunden'].loc[dfd['Neukunden']['Hausnr'].isnull(), 'Strasse'].apply(extract_hausnr)
        dfd['Neukunden']['Strasse'] = dfd['Neukunden']['Strasse'].str.split(expand=True)[0]
        # Convert Hausnr to int
        dfd['Neukunden']['Hausnr'] = pd.to_numeric(dfd['Neukunden']['Hausnr']).astype(int)
        
        # Geschlecht konsistent machen 
        #if not dfd['Neukunden']['Geschlecht'].filter(regex=).empty:
        dfd['Neukunden']['Geschlecht'] = dfd['Neukunden']['Geschlecht'].str.strip().replace(["^w$", "^W$"],'Weiblich', regex=True)
        dfd['Neukunden']['Geschlecht'] = dfd['Neukunden']['Geschlecht'].str.strip().replace(["^m$", "^M$"],'Männlich', regex=True)
        
        # Datumsspalten konsistent machen
        dfd['Neukunden']['Geburtsdatum'] = pd.to_datetime(dfd['Neukunden']['Geburtsdatum'],format='mixed', dayfirst=True)
        dfd['Neukunden']['PersoValidTo'] = pd.to_datetime(dfd['Neukunden']['PersoValidTo'],format='mixed', dayfirst=True)
        
        
    # ### Bestand
    if not df_bestand.empty:
        # Jahre zu int konvertieren
        df_bestand['Jahr'] = pd.to_numeric(df_bestand['Jahr']).astype('Int64', errors='ignore')
        
    
    
    
    
    
    
    
    # # Aufbau der Tabellen für das Core DWH
    
    dfs={}
    
    
    
    
    Kunde_cols={
        0:'ID_Kunde',
        1:'ID_Mitgliedsstatus',
        2:'Kundennr',
        3:'Vorname_1',
        4:'Vorname_2',
        5:'Nachname',
        6:'Anrede',
        7:'Titel',
        8:'PLZ',
        9:'Strasse',
       10:'Hausnr',
       11:'Mail',
       12:'Tel',
       13:'Geschlecht',
       14:'Geburtsdatum',
       15:'Beruf',
       16:'PersoNr',
       17:'PersoValidTo', 
       18:'Mitglied seit',
       19:'Mitglied bis' 
    }
    
    Bewertung_cols={
        1:'ID_Kunde',
        2:'ID_Buch',
        3:'Wertung',
        4:'Rezension'
    }
    
    Buch_cols={
        1:'ID_Kunde',
        2:'ID_Buch',
        3:'Wertung',
        4:'Rezension'
    }
    
    Exemplare_cols={
        1:'ID_Bestand',
        2:'Nr',
        3:'Kennung',
        4:'Zugriffsort',
        5:'Zustand'
    }
    
    Buch_cols={
        1:'ID_Autor',
        2:'Nr',
        3:'Titel',
        4:'Jahr',
        5:'Art',
    }
    
    Leihe_cols={
        0:'ID_Kunde',
        1:'ID_Exemplar',
        2:'Ausleihdatum',
        3:'Rueckgabedatum',
        4:'Verlaengerungsstatus',
        5:'Mahnstatus',
        6:'Fernleihe'
    }
    
    Mitgliedsstatus_cols={
        0:'ID_Mitgliedsstatus',
        2:'Bezeichnung',
        3:'Jahresbeitrag',
        4:'Mahnbetrag'
    }
    
    Beitragszahlung_cols={
        0:'ID_Zahlung',
        2:'ID_Kunde',
        3:'Datum',
        4:'Betrag',
    }
    
    
    
    
    
    dfs['Kunde']     = dfd['Neukunden'].reindex(columns=list(Kunde_cols.values()))
    dfs['Bewertung'] = dfd['Bewertung'].reindex(columns=list(Bewertung_cols.values()))
    dfs['Exemplare'] = df_bestand.reindex(columns=list(Exemplare_cols.values())).rename(columns={
                                                                            'ID_Bestand':'ID_Exemplar',
                                                                            'Nr':'ID_Buch'    
                                                                            })
    
    dfs['Autor']   = df_bestand[['Autor', 'Herkunft']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index':'ID_Autor'})
    
    dfs['Buch']    = pd.merge(df_bestand, 
                              dfs['Autor'], 
                              on=['Autor']
                             )[list(Buch_cols.values())]                                  \
                                                        .drop_duplicates()                \
                                                        .reset_index(drop=True)           \
                                                        .rename(columns={'Nr':'ID_Buch'})
    
    dfs['Leihe'] = dfd['Transaktion'].reindex(columns=list(Leihe_cols.values())).reset_index().rename(columns={'index':'ID_Leihe'})

    # Fülle die Werte von "Ausleihdatum" in DWH Tabelle "Leihe" mit den Werten von "Datum" aus Quelltabelle "Transaktion" wenn in der Spalte "Aktion" der Wert "Ausleihe" steht
    dfs['Leihe']['Ausleihdatum']=pd.to_datetime(dfs['Leihe']['Ausleihdatum'])
    dfs['Leihe'].loc[dfd['Transaktion']['Aktion'] == "Leihe", "Ausleihdatum"] = dfd['Transaktion']['Datum']
    # Analog "Rueckgabedatum" und "Rückgabe"
    dfs['Leihe']['Rueckgabedatum']=pd.to_datetime(dfs['Leihe']['Rueckgabedatum'])
    dfs['Leihe'].loc[dfd['Transaktion']['Aktion'] == "Rückgabe", "Rueckgabedatum"] = dfd['Transaktion']['Datum']

    
    ### more ###
    dfs['Mitgliedsstatus'] = pd.DataFrame(columns=list(Mitgliedsstatus_cols.values()))
    dfs['Beitragszahlung'] = dfd['Neukunden'].reindex(columns=list(Beitragszahlung_cols.values()))
    
    
    
    
    
    
    
    # # Schicke die Tabellen an den SQL Server
    # 
    

    def get_connection(database=DATABASE):
        return create_engine(
            url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}"\
                .format(USER, PASSWORD, HOST, PORT, database)
        )
    
    engine = get_connection('mysql')
    with engine.connect() as connection:
        insp = inspect(engine)
        db_list = insp.get_schema_names()
        print(f"Connection to the {HOST} for user {USER} created successfully.")
        if DATABASE not in db_list:
            sql = text(f"CREATE DATABASE {DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            result = connection.execute(sql)
            if result:
                print(f"Database {DATABASE} created!")
    
    







    if bestandskunden_flag == 1:
    
        df_bestandskunden = pd.read_csv('data/Kunde.csv', sep=',', encoding='unicode_escape')
        
        df_bestandskunden['Aktion'] = pd.Series(['Bestandskunde' for x in range(len(df_bestandskunden.index))])
        
        
        # IDs zu int konvertieren
        df_bestandskunden['ID_Kunde'] = pd.to_numeric(df_bestandskunden['ID_Kunde']).astype(int)
        df_bestandskunden['Kundennr'] = pd.to_numeric(df_bestandskunden['Kundennr']).astype(int)
        
        # Anreden konsistent machen 
        
        df_bestandskunden['Anrede'] = df_bestandskunden['Anrede'].replace(['Fr\.'],'Frau', regex=True)
        df_bestandskunden['Anrede'] = df_bestandskunden['Anrede'].replace(['Hr\.'],'Herr', regex=True)
        
        df_bestandskunden['Hausnr']  = np.where(df_bestandskunden['Hausnr'].astype(str).str.contains("NaN"), np.nan, df_bestandskunden['Hausnr'] )
        # 4-stellige PLZ mit "0" präfixen 
        df_bestandskunden['PLZ']    = df_bestandskunden['PLZ'].astype(str).str.strip().str.rjust(5, '0')
        
        # Hausnummern aus Strasse-Spalte extrahieren
        def extract_hausnr(strasse):
          if pd.isna(strasse):
            return np.nan
          parts = strasse.split()
          if len(parts) > 1 and parts[-1].isdigit():
            return parts[-1]
          else:
            return np.nan
        #Apply extract_hausnr, wenn 'Hausnr' NaN und entferne die Hausnummern aus der Strasse Spalte
        df_bestandskunden.loc[df_bestandskunden['Hausnr'].isnull(), 'Hausnr'] = df_bestandskunden.loc[df_bestandskunden['Hausnr'].isnull(), 'Strasse'].apply(extract_hausnr)
        df_bestandskunden['Strasse'] = df_bestandskunden['Strasse'].str.rsplit(expand=True)[0]
        # Convert Hausnr to int
        df_bestandskunden['Hausnr'] = pd.to_numeric(df_bestandskunden['Hausnr']).astype('Int64')
        
        # Geschlecht konsistent machen 
        df_bestandskunden['Geschlecht'] = df_bestandskunden['Geschlecht'].str.strip().replace(["^w$", "^W$"],'Weiblich', regex=True)
        df_bestandskunden['Geschlecht'] = df_bestandskunden['Geschlecht'].str.strip().replace(["^m$", "^M$"],'Männlich', regex=True)
        
        # Datumsspalten konsistent machen
        df_bestandskunden['Geburtsdatum'] = pd.to_datetime(df_bestandskunden['Geburtsdatum'],format='mixed', dayfirst=True)
        df_bestandskunden['PersoValidTo'] = pd.to_datetime(df_bestandskunden['PersoValidTo'],format='mixed', dayfirst=True)
        
        
        df_bestandskunden = df_bestandskunden.reindex(columns=list(Kunde_cols.values()))
        df_bestandskunden.to_sql('Kunde', con=engine,schema=DATABASE, index=False, if_exists='append')
        
        bestandskunden_flag=0


    
    
    #for Tabelle in dfs.keys():
    #    dfs[Tabelle].to_sql(Tabelle, con=engine,schema=DATABASE, index=False, if_exists='append') 
    
    
    CDW_Tabellen_Liste = insp.get_table_names()
    
    Statische_Tabellen = ['Exemplare', 'Autor', 'Buch']
    
    for Tabelle in Statische_Tabellen:
        if statische_tabellen_flag == 1:
            dfs[Tabelle].to_sql(Tabelle, con=engine,schema=DATABASE, index=False, if_exists='replace')
    
    statische_tabellen_flag = 0
    
    for Tabelle in dfs.keys():
        if Tabelle not in Statische_Tabellen:
            if not dfs[Tabelle].empty:
                dfs[Tabelle].to_sql(Tabelle, con=engine,schema=DATABASE, index=False, if_exists='append')     
        




    # create Marts
    if checkcount%20 == 0:
        
        query_M_Leihe="""
        drop database if exists M_Leihe;
        create schema M_Leihe;
        
        #--=============== Erstelle Dimensionstabellen ================--
        
        #--==== Tabelle D_Buch ====--
        
        DROP TABLE IF EXISTS M_Leihe.D_Buch;
        
         
         
        
        
        CREATE TABLE M_Leihe.D_Buch
        (
        ID_Exemplar integer Primary Key not null,
        Buch_ID integer, 
        Genre varchar(50)
        );
        
        INSERT INTO M_Leihe.D_Buch
        SELECT 
         DISTINCT(ID_Exemplar), E.ID_Buch, B.Art  
        FROM 
         test_db_1.Exemplare AS E
        JOIN 
         test_db_1.Buch AS B
        ON 
         E.ID_Buch = B.ID_Buch;
        
        
        
        
        
        
        
        #--SELECT * from M_Leihe.D_Buch;
        
        #--==== Tabelle D_Kundenalter ====--
        
        
        DROP TABLE IF EXISTS M_Leihe.D_Kundenalter;
        
        
        
        CREATE TABLE M_Leihe.D_Kundenalter
        (
        ID_Kundenalter integer AUTO_INCREMENT Primary Key not null, 
        Kundenalter    integer not null
        );
        
        
        INSERT INTO M_Leihe.D_Kundenalter (Kundenalter)
        SELECT 
         DISTINCT(TIMESTAMPDIFF(YEAR, Geburtsdatum, NOW()))
        FROM test_db_1.Kunde;
        
        
        
        #--SELECT * from M_Leihe.D_Kundenalter;
        
        
        #--==== Tabelle D_Kundenort ====--
        
        
        
        DROP TABLE IF EXISTS M_Leihe.D_Kundenort;
        
        
        
        CREATE TABLE M_Leihe.D_Kundenort
        (
        ID_Kundenort integer     AUTO_INCREMENT Primary Key not null, 
        PLZ          varchar(50)
        );
        
        
        INSERT INTO M_Leihe.D_Kundenort (PLZ)
        SELECT 
         DISTINCT(PLZ)
        FROM test_db_1.Kunde;
        
        
        
        #--SELECT * from M_Leihe.D_Kundenort;
        
        
        #--==== Tabelle D_Datum ====--
        
        
        
        
        DROP TABLE IF EXISTS M_Leihe.D_Datum;
        
        
        
        CREATE TABLE M_Leihe.D_Datum
        (
        ID_Datum        integer     AUTO_INCREMENT Primary Key not null, 
        Datum           datetime
        );
        #--Monat           integer,
        #--Monatsname      varchar(50),
        #--Quartal         integer,
        #--Jahr            integer
        #--);
        
        
        
        INSERT INTO M_Leihe.D_Datum (Datum)
        SELECT 
         DISTINCT(Ausleihdatum)
        FROM test_db_1.Leihe
        WHERE
         Ausleihdatum is not NULL;
        
        
        
        #--SELECT * from M_Leihe.D_Datum;
        
        #--=============== Erstelle Faktentabelle ================--
        
        #--==== Tabelle F_Leihe ====--
        
        
        
        
        DROP TEMPORARY TABLE IF EXISTS M_Leihe.T_Leihen;
        
        
        
        CREATE TEMPORARY TABLE M_Leihe.T_Leihen
        (
        select L1.ID_Kunde, L1.ID_Exemplar, L1.Ausleihdatum, L1.Rueckgabedatum from test_db_1.Leihe L1
        join test_db_1.Leihe L2
        on L1.ID_Kunde = L2.ID_Kunde AND L1.ID_Exemplar = L2.ID_Exemplar
        );
        
        SET @maxdate = (SELECT GREATEST(MAX(Ausleihdatum),MAX(Rueckgabedatum)) FROM test_db_1.Leihe LIMIT 1);
        
        DROP TEMPORARY TABLE IF EXISTS M_Leihe.T_Leihen_2;
        
        
        
        CREATE TEMPORARY TABLE M_Leihe.T_Leihen_2
        (
        SELECT
            ID_Kunde,
            ID_Exemplar,
            MAX(Ausleihdatum) AS Ausleihdatum,
            MAX(Rueckgabedatum) AS Rueckgabedatum,
            TIMESTAMPDIFF(DAY, MAX(Ausleihdatum), COALESCE(MAX(Rueckgabedatum),@maxdate)) AS Leihdauer
        FROM
            M_Leihe.T_Leihen
        GROUP BY
            ID_Kunde,
            ID_Exemplar
        );
        
        
        
        
        DROP TABLE IF EXISTS M_Leihe.F_Leihe;
        
        
        
        
        CREATE TABLE M_Leihe.F_Leihe
        (
        ID_Exemplar integer not null, 
        ID_Kundenalter integer  not null, 
        ID_Kundenort integer  not null, 
        ID_Datum integer  not null, 
        Leihdauer    integer
        );
        
        
        
        INSERT INTO M_Leihe.F_Leihe 
        SELECT L2.ID_Exemplar, KA.ID_Kundenalter, KO.ID_Kundenort, D.ID_Datum, L2.Leihdauer  FROM M_Leihe.T_Leihen_2 L2
        JOIN M_Leihe.D_Datum AS D
        ON L2.Ausleihdatum = D.Datum
        JOIN test_db_1.Kunde AS K
        ON K.ID_Kunde = L2.ID_Kunde
        JOIN M_Leihe.D_Kundenalter AS KA
        ON TIMESTAMPDIFF(YEAR, K.Geburtsdatum, NOW()) = KA.Kundenalter
        JOIN M_Leihe.D_Kundenort AS KO
        ON K.PLZ = KO.PLZ;
    
       
        
        ############################ hiert startet Verfügbarkeit
        

        
        DROP TEMPORARY TABLE IF EXISTS M_Leihe.T_Leihen;
        
        
        CREATE TEMPORARY TABLE M_Leihe.T_Leihen
        (
        select L1.ID_Kunde, L1.ID_Exemplar, L1.Ausleihdatum, L1.Rueckgabedatum from test_db_1.Leihe L1
        join test_db_1.Leihe L2
        on L1.ID_Kunde = L2.ID_Kunde AND L1.ID_Exemplar = L2.ID_Exemplar
        );
        
        
        
        SET @maxdate = (SELECT GREATEST(MAX(Ausleihdatum),MAX(Rueckgabedatum)) FROM test_db_1.Leihe LIMIT 1);
         
         
         
         
        
        DROP TEMPORARY TABLE IF EXISTS M_Leihe.T_Leihen_2;
        
        
        
        CREATE TEMPORARY TABLE M_Leihe.T_Leihen_2
        (
        SELECT
            ID_Kunde,
            ID_Exemplar,
            MAX(Ausleihdatum) AS Ausleihdatum,
            MAX(Rueckgabedatum) AS Rueckgabedatum,
            TIMESTAMPDIFF(DAY, MAX(Ausleihdatum), COALESCE(MAX(Rueckgabedatum),@maxdate)) AS Leihdauer
        FROM
            M_Leihe.T_Leihen
        GROUP BY
            ID_Kunde,
            ID_Exemplar
        );
        

        
        CREATE SCHEMA IF NOT EXISTS M_Verfuegbarkeit;
        
        DROP TABLE IF EXISTS M_Verfuegbarkeit.D_Buch;
        
        CREATE TABLE M_Verfuegbarkeit.D_Buch
        (
        ID_BUCH integer primary key, 
        Genre varchar(50),
        Herkunft varchar(50)
        );
        
        INSERT INTO M_Verfuegbarkeit.D_Buch
        SELECT 
         B.ID_Buch, B.Art, A.Herkunft  
        FROM 
         test_db_1.Buch AS B
        JOIN 
         test_db_1.Autor AS A
        ON 
         B.ID_Autor=A.ID_Autor
        WHERE A.Autor is not NULL;
        
        SET @mindate = (SELECT MIN(Ausleihdatum) FROM M_Leihe.T_Leihen_2 LIMIT 1);
        SET @maxdate = (SELECT GREATEST(MAX(Ausleihdatum),MAX(Rueckgabedatum)) FROM M_Leihe.T_Leihen_2 LIMIT 1);
        SET @timespan = DATEDIFF(@maxdate,@mindate);
        SELECT @timespan;
        
        DROP TEMPORARY TABLE IF EXISTS M_Verfuegbarkeit.EX_VERF;
        CREATE TEMPORARY TABLE M_Verfuegbarkeit.EX_VERF
        (SELECT ID_Exemplar,  GREATEST(0.1,1 - SUM(DATEDIFF(COALESCE(Rueckgabedatum, @maxdate), Ausleihdatum))/@timespan)  AS Verfugbarkeit 
        FROM M_Leihe.T_Leihen_2
        GROUP BY ID_Exemplar);
        
        
        DROP TABLE IF EXISTS  M_Verfuegbarkeit.F_Verfuegbarkeit;
        
        CREATE TABLE IF NOT EXISTS M_Verfuegbarkeit.F_Verfuegbarkeit
        (
        ID_Buch integer not null, 
        Verfuegbarkeit float
        );
        
        INSERT INTO M_Verfuegbarkeit.F_Verfuegbarkeit 
        SELECT ID_Buch, AVG(v.Verfugbarkeit) AS Verfuegbarkeit
        FROM M_Verfuegbarkeit.EX_VERF v
        JOIN test_db_1.Exemplare e
        ON v.ID_Exemplar = e.ID_Exemplar
        GROUP BY ID_Buch;
    






        #### Erstelle Mart M_Bewertung
        DROP SCHEMA IF EXISTS M_Bewertung;
        
        CREATE SCHEMA IF NOT EXISTS M_Bewertung;
         
         
        
        #--==== Tabelle D_Buch ====--
        
        DROP TABLE IF EXISTS M_Bewertung.D_Buch;
        
            
        CREATE TABLE M_Bewertung.D_Buch
        (
        ID_Exemplar integer Primary Key not null,
        Buch_ID integer, 
        Genre varchar(50)
        );
        
        INSERT INTO M_Bewertung.D_Buch
        SELECT 
        DISTINCT(ID_Exemplar), E.ID_Buch, B.Art  
        FROM 
        test_db_1.Exemplare AS E
        JOIN 
        test_db_1.Buch AS B
        ON 
        E.ID_Buch = B.ID_Buch;
        
        
        #--==== Tabelle D_Kundenalter ====--
        
        
        DROP TABLE IF EXISTS M_Bewertung.D_Kundenalter;
        
        
        
        CREATE TABLE M_Bewertung.D_Kundenalter
        (
        ID_Kundenalter integer AUTO_INCREMENT Primary Key not null, 
        ID_Kunde integer,
        Kundenalter    integer not null
        );
        
        
        INSERT INTO M_Bewertung.D_Kundenalter (ID_Kunde, Kundenalter)
        SELECT 
            DISTINCT ID_Kunde, TIMESTAMPDIFF(YEAR, Geburtsdatum, NOW())
        FROM test_db_1.Kunde;
        
        
        
        #--SELECT * from M_Bewertung.D_Kundenalter;
        
        
        #--==== Tabelle D_Kundenort ====--
        
        
        
        DROP TABLE IF EXISTS M_Bewertung.D_Kundenort;
        
        
        
        CREATE TABLE M_Bewertung.D_Kundenort
        (
        ID_Kundenort integer     AUTO_INCREMENT Primary Key not null, 
        ID_Kunde integer,
        PLZ          varchar(50)
        );
        
        
        INSERT INTO M_Bewertung.D_Kundenort (ID_Kunde , PLZ)
        SELECT 
            DISTINCT ID_Kunde, PLZ
        FROM test_db_1.Kunde;
        
        
        
        
        #--==== Tabelle D_Datum ====--
        
        
        
        
        DROP TABLE IF EXISTS M_Bewertung.D_Datum;
        
        
        
        CREATE TABLE M_Bewertung.D_Datum
        (
        ID_Datum        integer     AUTO_INCREMENT Primary Key not null, 
        Datum           datetime
        );
        #--Monat           integer,
        #--Monatsname      varchar(50),
        #--Quartal         integer,
        #--Jahr            integer
        #--);
        
        
        
        INSERT INTO M_Bewertung.D_Datum (Datum)
        SELECT 
            DISTINCT(Ausleihdatum)
        FROM test_db_1.Leihe
        WHERE
            Ausleihdatum is not NULL;
        
        
        
        #--SELECT * from M_Bewertung.D_Datum;
        
        
        #--=============== Erstelle Faktentabelle ================--
        
        #--==== Tabelle F_Leihe ====--
        
        
        
        
        DROP TEMPORARY TABLE IF EXISTS M_Bewertung.T_Leihen;
        
        CREATE TEMPORARY TABLE M_Bewertung.T_Leihen
        (
        select L1.ID_Kunde, L1.ID_Exemplar, L1.Ausleihdatum, L1.Rueckgabedatum from test_db_1.Leihe L1
        join test_db_1.Leihe L2
        on L1.ID_Kunde = L2.ID_Kunde AND L1.ID_Exemplar = L2.ID_Exemplar
        );
        
        
        SET @maxdate = (SELECT GREATEST(MAX(Ausleihdatum),MAX(Rueckgabedatum)) FROM test_db_1.Leihe LIMIT 1);
         
         
         
         
        
        DROP TEMPORARY TABLE IF EXISTS M_Bewertung.T_Leihen_2;
        
        
        
        CREATE TEMPORARY TABLE M_Bewertung.T_Leihen_2
        (
        SELECT
            ID_Kunde,
            ID_Exemplar,
            MAX(Ausleihdatum) AS Ausleihdatum,
            MAX(Rueckgabedatum) AS Rueckgabedatum,
            TIMESTAMPDIFF(DAY, MAX(Ausleihdatum), COALESCE(MAX(Rueckgabedatum),@maxdate)) AS Leihdauer
        FROM
            M_Bewertung.T_Leihen
        GROUP BY
            ID_Kunde,
            ID_Exemplar
        );
        
     
        
        
        DROP TABLE IF EXISTS M_Bewertung.F_Bewertung;
        
        CREATE TABLE M_Bewertung.F_Bewertung
        (
        ID_Buch integer not null, 
        ID_Kundenalter integer  not null, 
        ID_Kundenort integer  not null, 
        Wertung    integer
        );
        
        INSERT INTO M_Bewertung.F_Bewertung 
        SELECT DISTINCT BE.ID_Buch, KA.Kundenalter , KO.ID_Kundenort , BE.Wertung
        FROM test_db_1.Bewertung BE
        LEFT JOIN test_db_1.Buch BU
        ON BE.ID_Buch = BU.ID_Buch
        LEFT JOIN M_Bewertung.D_Kundenort KO 
        ON BE.ID_Kunde = KO.ID_Kunde
        LEFT JOIN M_Bewertung.D_Kundenalter KA 
        ON BE.ID_Kunde = KA.ID_Kunde;
                 
        DROP TABLE IF EXISTS F_Verfuegbarkeit.T_Test;

        """
    
    
        mysql_connection = connect(user=USER, password=PASSWORD, host=HOST, port=PORT, autocommit=True )#, database=DATABASE)
        cursor = mysql_connection.cursor()
        cursor.execute(query_M_Leihe, multi=True)
        cursor.close() 


