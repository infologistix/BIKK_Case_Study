#--drop database test_db_1;
#--drop database M_Leihe;

#--==================================================--
#--=============== Erstelle Mart M_Leihe ================--
#--==================================================--

create schema M_Leihe;

#--=============== Erstelle Dimensionstabellen ================--

#--==== Tabelle D_Buch ====--

DROP TABLE IF EXISTS M_Leihe.D_Buch;

 
 
GO

CREATE TABLE M_Leihe.D_Buch
(
ID_Exemplar integer Primary Key not null,
Buch_ID integer, 
Genre varchar(50),
Herkunft varchar(50)
);

INSERT INTO M_Leihe.D_Buch
SELECT 
 DISTINCT(E.ID_Exemplar), E.ID_Buch, B.Art, A.Herkunft  
FROM 
 test_db_1.Exemplare AS E
JOIN 
 test_db_1.Buch AS B
ON 
 E.ID_Buch = B.ID_Buch
JOIN 
 test_db_1.Autor AS A
ON 
 B.ID_Autor=A.ID_Autor
WHERE A.Autor is not NULL




select * from M_Leihe.D_Buch

GO

#--SELECT * from M_Leihe.D_Buch;

#--==== Tabelle D_Kundenalter ====--


DROP TABLE IF EXISTS M_Leihe.D_Kundenalter;

GO

CREATE TABLE M_Leihe.D_Kundenalter
(
ID_Kundenalter integer AUTO_INCREMENT Primary Key not null, 
Kundenalter    integer not null
);


INSERT INTO M_Leihe.D_Kundenalter (Kundenalter)
SELECT 
 DISTINCT(TIMESTAMPDIFF(YEAR, Geburtsdatum, NOW()))
FROM test_db_1.Kunde;

GO

#--SELECT * from M_Leihe.D_Kundenalter;


#--==== Tabelle D_Kundenort ====--



DROP TABLE IF EXISTS M_Leihe.D_Kundenort;

GO

CREATE TABLE M_Leihe.D_Kundenort
(
ID_Kundenort integer     AUTO_INCREMENT Primary Key not null, 
PLZ          varchar(50)
);


INSERT INTO M_Leihe.D_Kundenort (PLZ)
SELECT 
 DISTINCT(PLZ)
FROM test_db_1.Kunde;

GO

#--SELECT * from M_Leihe.D_Kundenort;


#--==== Tabelle D_Datum ====--




DROP TABLE IF EXISTS M_Leihe.D_Datum;

GO

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

GO

INSERT INTO M_Leihe.D_Datum (Datum)
SELECT 
 DISTINCT(Ausleihdatum)
FROM test_db_1.Leihe
WHERE
 Ausleihdatum is not NULL;

GO

#--SELECT * from M_Leihe.D_Datum;

#--=============== Erstelle Faktentabelle ================--

#--==== Tabelle F_Leihe ====--




DROP TEMPORARY TABLE IF EXISTS M_Leihe.T_Leihen;

GO

CREATE TEMPORARY TABLE M_Leihe.T_Leihen
(
select L1.ID_Kunde, L1.ID_Exemplar, L1.Ausleihdatum, L1.Rueckgabedatum from test_db_1.Leihe L1
join test_db_1.Leihe L2
on L1.ID_Kunde = L2.ID_Kunde AND L1.ID_Exemplar = L2.ID_Exemplar
);

GO 

DROP TEMPORARY TABLE IF EXISTS M_Leihe.T_Leihen_2;

GO

CREATE TEMPORARY TABLE M_Leihe.T_Leihen_2
(
SELECT
    ID_Kunde,
    ID_Exemplar,
    MAX(Ausleihdatum) AS Ausleihdatum,
    MAX(Rueckgabedatum) AS Rueckgabedatum,
    TIMESTAMPDIFF(DAY, MAX(Ausleihdatum), MAX(Rueckgabedatum)) AS Leihdauer
FROM
    M_Leihe.T_Leihen
GROUP BY
    ID_Kunde,
    ID_Exemplar
);

GO


DROP TABLE IF EXISTS M_Leihe.F_Leihe;

GO


CREATE TABLE M_Leihe.F_Leihe
(
ID_Exemplar integer not null, 
ID_Kundenalter integer  not null, 
ID_Kundenort integer  not null, 
ID_Datum integer  not null, 
Kundenalter    integer
);

GO

INSERT INTO M_Leihe.F_Leihe 
SELECT L2.ID_Exemplar, KA.ID_Kundenalter, KO.ID_Kundenort, D.ID_Datum, L2.Leihdauer  FROM M_Leihe.T_Leihen_2 L2
JOIN M_Leihe.D_Datum AS D
ON L2.Ausleihdatum = D.Datum
JOIN test_db_1.Kunde AS K
ON K.ID_Kunde = L2.ID_Kunde
JOIN M_Leihe.D_Kundenalter AS KA
ON TIMESTAMPDIFF(YEAR, K.Geburtsdatum, NOW()) = KA.Kundenalter
JOIN M_Leihe.D_Kundenort AS KO
ON K.PLZ = KO.PLZ

#-- select * from M_Leihe.F_Leihe 
######




















############################ hiert startet Verfügbarkeit


DROP SCHEMA M_Verfuegbarkeit;


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

DROP TEMPORARY TABLE IF EXISTS EX_VERF;
CREATE TEMPORARY TABLE EX_VERF
(SELECT ID_Exemplar,  GREATEST(0.1,1 - SUM(DATEDIFF(COALESCE(Rueckgabedatum, @maxdate), Ausleihdatum))/@timespan)  AS Verfugbarkeit 
FROM M_Leihe.T_Leihen_2
GROUP BY ID_Exemplar);




CREATE TABLE IF NOT EXISTS M_Verfuegbarkeit.F_Verfuegbarkeit
(
ID_Buch integer not null, 
Verfuegbarkeit float not null
);

INSERT INTO M_Verfuegbarkeit.F_Verfuegbarkeit 
SELECT ID_Buch, AVG(v.Verfugbarkeit) AS Verfuegbarkeit
FROM EX_VERF v
JOIN test_db_1.Exemplare e
ON v.ID_Exemplar = e.ID_Exemplar
GROUP BY ID_Buch;


###### hier endet M_Verfuegbarkeit



###### hier beginnt M_Bewertung ######

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
Kundenalter    integer not null
);


INSERT INTO M_Bewertung.D_Kundenalter (Kundenalter)
SELECT 
    DISTINCT(TIMESTAMPDIFF(YEAR, Geburtsdatum, NOW()))
FROM test_db_1.Kunde;



#--SELECT * from M_Bewertung.D_Kundenalter;


#--==== Tabelle D_Kundenort ====--



DROP TABLE IF EXISTS M_Bewertung.D_Kundenort;



CREATE TABLE M_Bewertung.D_Kundenort
(
ID_Kundenort integer     AUTO_INCREMENT Primary Key not null, 
PLZ          varchar(50)
);


INSERT INTO M_Bewertung.D_Kundenort (PLZ)
SELECT 
    DISTINCT(PLZ)
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

DROP TEMPORARY TABLE IF EXISTS M_Bewertung.T_Leihen_2;

CREATE TEMPORARY TABLE M_Bewertung.T_Leihen_2
(
SELECT
    ID_Kunde,
    ID_Exemplar,
    MAX(Ausleihdatum) AS Ausleihdatum,
    MAX(Rueckgabedatum) AS Rueckgabedatum,
    TIMESTAMPDIFF(DAY, MAX(Ausleihdatum), MAX(Rueckgabedatum)) AS Leihdauer
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
ID_Datum integer  not null, 
Wertung    integer
);

INSERT INTO M_Bewertung.F_Bewertung 
SELECT DISTINCT BE.ID_Buch, KA.ID_Kundenalter , KO.ID_Kundenort,  D.ID_Datum, BE.Wertung
FROM test_db_1.Bewertung BE
JOIN test_db_1.Buch BU
ON BE.ID_Buch = BU.ID_Buch
JOIN M_Bewertung.T_Leihen_2 L2
ON BE.ID_Kunde = L2.ID_Kunde
JOIN M_Bewertung.D_Datum AS D
ON L2.Ausleihdatum = D.Datum
JOIN test_db_1.Kunde AS K
ON K.ID_Kunde = L2.ID_Kunde
JOIN M_Bewertung.D_Kundenalter AS KA
ON TIMESTAMPDIFF(YEAR, K.Geburtsdatum, NOW()) = KA.Kundenalter
JOIN M_Bewertung.D_Kundenort AS KO
ON K.PLZ = KO.PLZ;





### testen

SELECT DISTINCT BE.ID_Buch, KA.ID_Kundenalter , KO.ID_Kundenort,  D.ID_Datum, BE.Wertung
FROM test_db_1.Bewertung BE
JOIN test_db_1.Buch BU
ON BE.ID_Buch = BU.ID_Buch
JOIN M_Bewertung.T_Leihen_2 L2
ON BE.ID_Kunde = L2.ID_Kunde
JOIN M_Bewertung.D_Datum AS D
ON L2.Ausleihdatum = D.Datum
JOIN test_db_1.Kunde AS K
ON K.ID_Kunde = L2.ID_Kunde
JOIN M_Bewertung.D_Kundenalter AS KA
ON TIMESTAMPDIFF(YEAR, K.Geburtsdatum, NOW()) = KA.Kundenalter
JOIN M_Bewertung.D_Kundenort AS KO
ON K.PLZ = KO.PLZ;


JOIN M_Bewertung.D_Datum AS D
ON L2.Ausleihdatum = D.Datum
JOIN test_db_1.Kunde AS K
ON K.ID_Kunde = L2.ID_Kunde
JOIN M_Bewertung.D_Kundenalter AS KA
ON TIMESTAMPDIFF(YEAR, K.Geburtsdatum, NOW()) = KA.Kundenalter
JOIN M_Bewertung.D_Kundenort AS KO
ON K.PLZ = KO.PLZ;





select * from test_db_1.Bewertung BE
select * from M_Bewertung.T_Leihen_2 L2

JOIN M_Bewertung.T_Leihen_2 L2
ON BE.ID_Kunde = L2.ID_Kunde


JOIN test_db_1.Kunde AS K
ON K.ID_Kunde = L2.ID_Kunde

select * from M_Bewertung.T_Leihen_2 L2

JOIN M_Bewertung.D_Kundenalter AS KA
ON TIMESTAMPDIFF(YEAR, K.Geburtsdatum, NOW()) = KA.Kundenalter






select * from M_Bewertung.T_Leihen_2 L2


















######





select * from M_Leihe.D_Kundenalter;

JOIN test_db_1.Leihe AS L
ON 





select * from test_db_1.Exemplare



select * from test_db_1.Kunde


select * from test_db_1.Buch

select * from test_db_1.Leihe

select * from test_db_1.Autor

select * from test_db_1.Beitragszahlung








###


        
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
        
        DROP TABLE IF EXISTS  M_Verfuegbarkeit.F_Verfuegbarkeit
        
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
        
        
select * from M_Verfuegbarkeit.F_Verfuegbarkeit 






####################















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
        ON BE.ID_Kunde = KA.ID_Kunde
        

 