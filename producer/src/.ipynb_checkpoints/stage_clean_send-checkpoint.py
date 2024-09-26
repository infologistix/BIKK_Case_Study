

import numpy as np
import pandas as pd
from confluent_kafka import Producer
from connect import get_connection
from random import randint
from simulation import transaktion_factory
from time import sleep
import socket


from confluent_kafka import Consumer


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
    3:'ID_Exemplar',
    4:'ID_Kunde',
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





while 1:
    
    dfd={}
    
    # # Konsumiere Daten
        
    for col, topic in zip(cols, topics):
        print(col, topic)
    
        dfd[topic] = pd.DataFrame(columns=list(col.values()))
        
        
        c.subscribe([topic])
        
        for i in range(60):
            msg = c.poll(1)
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
    
    
    ### more ###
    dfs['Mitgliedsstatus'] = pd.DataFrame(columns=list(Mitgliedsstatus_cols.values()))
    dfs['Beitragszahlung'] = dfd['Neukunden'].reindex(columns=list(Beitragszahlung_cols.values()))
    
    
    
    
    
    
    
    # # Schicke die Tabellen an den SQL Server
    # 
    
    
    from sqlalchemy import create_engine, inspect, text
    
    USER = 'root'
    PASSWORD = 'root'
    HOST = 'mysql57' # 'localhost'          # see yml file
    PORT = 3306 # 3307
    DATABASE = 'test_db_1'
     
    
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
    
    
    
    
    for Tabelle in dfs.keys():
        dfs[Tabelle].to_sql(Tabelle, con=engine,schema=DATABASE, index=False, if_exists='append') 
    
    
    


