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

DROP TEMPORARY TABLE IF EXISTS M_Leihe.T_Leihen_2;

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





SELECT L2.ID_Exemplar, KA.ID_Kundenalter, KO.ID_Kundenort, D.ID_Datum, L2.Leihdauer  FROM M_Leihe.T_Leihen_2 L2
JOIN M_Leihe.D_Datum AS D
ON L2.Ausleihdatum = D.Datum
JOIN test_db_1.Kunde AS K
ON K.ID_Kunde = L2.ID_Kunde
JOIN M_Leihe.D_Kundenalter AS KA
ON TIMESTAMPDIFF(YEAR, K.Geburtsdatum, NOW()) = KA.Kundenalter
JOIN M_Leihe.D_Kundenort AS KO
ON K.PLZ = KO.PLZ


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