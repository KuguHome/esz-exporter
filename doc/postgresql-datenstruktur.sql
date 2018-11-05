-- Schema szdaten

-- explizit in dieses Schema wechseln
set search_path = szdaten,public;

-- Kontrolle
select current_schema;

CREATE TABLE szdaten.vereinigung_element (
	sz_nummer				SMALLINT not null,
	elem_nummer			SMALLINT not null,
	vereinelem_nummer		INTEGER not null,
	vereinelem_von_datum	DATE not null,
	vereinelem_bis_datum		DATE null,
	vereinelem_wert_versatz	NUMERIC(9,3) not null,
	vereinelem_kommentar	VARCHAR(1024) null,

	CONSTRAINT vereinigung_element_pk PRIMARY KEY (sz_nummer, elem_nummer, vereinelem_nummer),
	CONSTRAINT vereinigung_element_name_eindeutig UNIQUE (elem_nummer, vereinelem_nummer),
	CONSTRAINT vereinigung_element_fk FOREIGN KEY (sz_nummer, elem_nummer) REFERENCES szdaten.element (sz_nummer, elem_nummer)
) WITHOUT OIDS;

-- Schema kugu

CREATE SCHEMA kugu AUTHORIZATION admin;

-- explizit in dieses Schema wechseln
set search_path = kugu,public;

-- Kontrolle
select current_schema;

CREATE TABLE kugu.kunde (
	kunde_nummer			SERIAL4 not null,
	kunde_bezeichnung		VARCHAR(50) not null,
	kunde_crm_firma_name	VARCHAR(100) not null,
	kunde_kommentar		VARCHAR(1024) null,
	
	CONSTRAINT kunde_pk PRIMARY KEY (kunde_nummer),
	CONSTRAINT kunde_crm_firma_name_eindeutig UNIQUE (kunde_crm_firma_name)
) WITHOUT OIDS;

-- Schema esz

CREATE SCHEMA esz AUTHORIZATION admin;

-- explizit in dieses Schema wechseln
set search_path = esz,public;

-- Kontrolle
select current_schema;

CREATE TABLE esz.endkunde (
	kunde_nummer			INTEGER not null, 	-- kugu.kunde.kunde_nummer GZ 9
	endku_system_art			SMALLINT not null,	-- GZ 1
	endku_groesse			SMALLINT not null, 	-- GZ 1
	endku_branche			SMALLINT not null, 	-- GZ 4
	endku_postleitzahl		SMALLINT not null, 	-- GZ 5	-- führende Null bei Export
	endku_bewohner_anzahl	SMALLINT not null, 	-- GZ 6	-- bis 100.000 Leute
	endku_gebaeude_typ		SMALLINT not null, 	-- GZ 1
	endku_baualterklasse		SMALLINT not null, 	-- GZ 1
	endku_sanierung_energ_letzte	SMALLINT not null, 	-- GZ 4
	endku_flaeche_gesamt		SMALLINT not null, 	-- GZ 10
	endku_smg_status 		SMALLINT not null, 	-- GZ 1

	CONSTRAINT endkunde_pk PRIMARY KEY (kunde_nummer),
	-- Detailbeschränkungen für Wertebereiche der jeweilgen Felder - oder auf Applikationsebene lösen. Gilt generell.
	CONSTRAINT element_fk FOREIGN KEY (kunde_nummer) REFERENCES kugu.kunde (kunde_nummer)
) WITHOUT OIDS;

CREATE TABLE esz.zaehler (
	kunde_nummer INTEGER not null,	--kugu.kunde.kunde_nummer
	zaehler_nummer			SERIAL4 not null,
	---
	vereinelem_nummer INTEGER not null,	--szdaten.vereinigung_element.vereinelem_nummer
	zaehler_versatz			NUMERIC(9,3) not null,
	zaehler_id				SMALLINT not null, 	-- GZ 10
	zaehler_id_uebergeordnet	SMALLINT not null, 	-- GZ 10
	zaehler_art				SMALLINT not null, 	-- GZ 1
	zaehler_geraete_art		SMALLINT not null,	-- GZ 6
	zaehler_geraete_sonstiges_freitext VARCHAR(256) null, 	-- Wort 256
	zaehler_typ				SMALLINT not null, 	-- GZ 1
	zaehler_energietraeger 	SMALLINT not null, 	-- GZ 1

	CONSTRAINT zaehler_pk PRIMARY KEY (kunde_nummer, zaehler_nummer),
	CONSTRAINT zaehler_nummer_eindeutig UNIQUE (kunde_nummer, zaehler_nummer),
	CONSTRAINT vereinelem_nummer_eindeutig UNIQUE (vereinelem_nummer),
	CONSTRAINT zaehler_fk FOREIGN KEY (kunde_nummer) REFERENCES kugu.kunde (kunde_nummer)
) WITHOUT OIDS;

CREATE TABLE esz.optimierung (
	kunde_nummer			INTEGER not null,		--kugu.kunde.kunde_nummer
	zaehler_nummer			INTEGER not null, 	--esz.zaehler.zaehler_nummer
	opt_nummer				SERIAL4 not null,
	opt_datum				DATE not null,		-- darüber opt. Bezug zu Messung
	opt_datum_aufgehoben	DATE null,			-- falls eine Maßnahme rückgängig gemacht wird
	opt_massnahme			SMALLINT null, 		-- GZ 4
	opt_sonstiges_freitext		VARCHAR(256) null, 	-- WORT 256

	CONSTRAINT optimierung_pk PRIMARY KEY (kunde_nummer, zaehler_nummer, opt_nummer),
	CONSTRAINT opt_nummer_eindeutig UNIQUE (zaehler_nummer, opt_nummer),
	CONSTRAINT optimierung_fk FOREIGN KEY (kunde_nummer, zaehler_nummer) REFERENCES esz.zaehler (kunde_nummer, zaehler_nummer)
) WITHOUT OIDS;

CREATE TABLE esz.messung (
	kunde_nummer			INTEGER not null, -- kugu.kunde.kunde_nummer --GZ 9
	zaehler_nummer			INTEGER not null,	-- esz.zaehler.zaehler_nummer
	mess_nummer			SERIAL4 not null, 	--GZ 12
	-- ^ = für Feld "laufende Nummer" im Export; ist eindeutig bzw. fortlaufend.
	mess_datum				DATE not null,
	-- es wird immer um 00:00 gemessen
	mess_zaehlerstand		NUMERIC(9,3) not null,

	CONSTRAINT messung_pk PRIMARY KEY (kunde_nummer, zaehler_nummer, mess_nummer),
	CONSTRAINT mess_nummer_eindeutig UNIQUE (zaehler_nummer, mess_nummer),
	CONSTRAINT mess_datum_eindeutig UNIQUE (mess_nummer, mess_datum),
	CONSTRAINT mess_datum_je_zaehler_eindeutig UNIQUE (zaehler_nummer, mess_datum),
	CONSTRAINT messung_fk FOREIGN KEY (kunde_nummer, zaehler_nummer) REFERENCES esz.zaehler (kunde_nummer, zaehler_nummer)
) WITHOUT OIDS;

CREATE TYPE esz.zaehlersum_zeitraum_art_enum AS ENUM ('Basislinie', 'jährlich');

CREATE TABLE esz.zaehlersumme (
	kunde_nummer				INTEGER not null,	--kugu.kunde.kunde_nummer
	zaehler_nummer				INTEGER not null,	--zaehler.zaehler_nummer
	zaehlersum_nummer			SERIAL4 not null,
	zaehlersum_zeitraum_art		zaehlersum_zeitraum_art_enum not null,
	zaehlersum_von_datum		DATE not null,
	zaehlersum_bis_datum			DATE not null,
	zaehlersum_summe			NUMERIC(9,3) not null,
	zaehlersum_bereinigungsfaktor 	NUMERIC(3,2) not null, 	-- RZ 3 = 3,14 z.B.

	CONSTRAINT zaehlersumme_pk PRIMARY KEY (kunde_nummer, zaehler_nummer, zaehlersum_nummer),
	CONSTRAINT zaehlersum_nummer_eindeutig UNIQUE (zaehler_nummer, zaehlersum_nummer),
	CONSTRAINT zaehlersum_fk FOREIGN KEY (kunde_nummer, zaehler_nummer) REFERENCES esz.zaehler (kunde_nummer, zaehler_nummer)
) WITHOUT OIDS;

CREATE TABLE esz.klimafaktor (
	klimafak_nummer 	SERIAL4 not null,
	klimafak_postleitzahl	SMALLINT not null,
	klimafak_von_datum	DATE not null,
	klimafak_bis_datum	DATE not null,
	klimafak_faktor		NUMERIC(3,2) not null,		--RZ 3 = 3,14

	CONSTRAINT klimafaktor_pk PRIMARY KEY (klimafak_nummer),
	CONSTRAINT faktor_eindeutig UNIQUE (klimafak_postleitzahl, klimafak_von_datum, klimafak_bis_datum)
) WITHOUT OIDS;

CREATE TABLE esz.export (
	konfig_antragsteller_nummer INTEGER not null,	-- Applikationskonstante
	mess_nummer		INTEGER not null, 		--messung.mess_nummer
	kunde_nummer		INTEGER not null, 		--endkunde.kunde_nummer
	endku_system_art		SMALLINT not null, 		--endkunde.endku_system_art
	endku_groesse		SMALLINT not null, 		--endkunde.endku_groesse
	endku_branche		SMALLINT not null, 		--endkunde.endku_branche
	endku_postleitzahl	SMALLINT not null, 		--endkunde.endku_postleitzahl
	endku_bewohner_anzahl	SMALLINT not null,	--endkunde.endku_bewohner_anzahl
	endku_gebaeude_typ	SMALLINT not null, 		--endkunde.endku_gebaeude_typ
	endku_baualterklasse	SMALLINT not null, 		-- endkunde.endku_baualterklasse
	endku_sanierung_energ_letzte	SMALLint not null,	--endkunde.endku_sanierung_energ_letzte
	endku_flaeche_gesamt	SMALLINT not null, 		--endkunde.endku_flaeche_gesamt
	mess_datum	DATE not null, 				--GZ 8 --messung.mess_datum
	-- ^ muss für Ausgabe dann in JJJMMTT konvertiert werden
	zahler_id				SMALLINT not null, 		--zaehler.zaehler_id
	zaehler_id_uebergeordnet	SMALLINT not null, 	--zaehler.zaehler_id_uebergeordnet
	zaehler_art			SMALLINT not null, 		--zaehler.zaehler_art
	zaehler_geraete_art	SMALLINT not null, 		--zaehler.zaehler_geraete_art
	zaehler_geraete_sonstiges_freitext	VARCHAR(256) null,	--zaehler.zaehler_geraete_sonstiges_freitext
	zaehler_typ			SMALLINT not null, 		--zaehler.zaehler_typ
	zaehler_energietraeger	SMALLINT not null, 		--zaehler.zaehler_energietraeger
	mess_zaehlerstand	NUMERIC(9,3) not null, 	--messung.mess_zaehlerstand
	opt_massnahme SMALLINT not null, 	--optimierung.opt_massnahme
	opt_sonstiges_freitext VARCHAR(256) null, 	--optimierung.opt_sonstiges_freitext
	endku_smg_status		SMALLINT not null, 	--endkunde.endku_smg_status
	zaehlersum_summe_eingespart NUMERIC(9,3) not null, 	--zaehlersumme.zaehlersum_summe_eingespart
	zaehlersum_summe_eingespart_bereinigt NUMERIC(9,3) not null, 	--zaehlersumme.zaehlersum_summe_eingespart_bereinigt
	konfig_abfrage_haeufigkeit SMALLINT not null, 	-- Applikationskonstante
	klimafak_faktor		NUMERIC(3,2) not null, 	--klimafaktor.klimafak_faktor
	export_einfluss1_wert	SMALLINT null,		--entfällt (leer im Export)
	export_einfluss1_art	SMALLINT null,		--entfällt
	export_einfluss2_wert	SMALLINT null,		--entfällt
	export_einfluss2_art	SMALLINT null,		--entfällt
	export_einfluss3_wert	SMALLINT null,		--entfällt
	export_einfluss3_art	SMALLINT null,		--entfällt
	export_einfluss4_wert	SMALLINT null,		--entfällt
	export_einfluss4_art	SMALLINT null,		--entfällt
	export_nutzen1_wert	SMALLINT null,		--entfällt
	export_nutzen1_art	SMALLINT null,		--entfällt
	export_nutzen2_wert	SMALLINT null,		--entfällt
	export_nutzen2_art	SMALLINT null,		--entfällt
	zaehlersum_summe_basislinie	NUMERIC(9,3) not null, 	--zaehlersumme.zaehlersum_summe_basislinie
	zaehlersum_summe_basislinie_bereinigt	NUMERIC(9,3) not null, 	--zaehlersumme.zaehlersum_summe_basislinie_bereinigt
	export_einfluss1_basislinie_gemittelt	SMALLINT null,	--entfällt
	export_einfluss2_basislinie_gemittelt	SMALLINT null,	--entfällt
	export_einfluss3_basislinie_gemittelt	SMALLINT null,	--entfällt
	export_einfluss4_basislinie_gemittelt	SMALLINT null,	--entfällt
	export_nutzen1_basislinie_gemittelt	SMALLINT null,	--entfällt
	export_nutzen2_basislinie_gemittelt	SMALLINT null,	--entfällt

	CONSTRAINT export_pk PRIMARY KEY (mess_nummer)
) WITHOUT OIDS;

-- Anbindung Steuerzentrale an kugu.kunde;

-- explizit in dieses Schema wechseln
set search_path = szdaten,public;

-- Kontrolle
select current_schema;

-- szdaten.steuerzentrale
ALTER TABLE szdaten.steuerzentrale 
ADD COLUMN kunde_nummer INTEGER null;	-- TODO Kann später noch geändert werden. Test-SZ, Entwickler-SZ und Test-Benutzer für DB-Eingang sollen keine Kundenzuordnung benötigen.

ALTER TABLE szdaten.steuerzentrale 
ADD CONSTRAINT steuerzentrale_kunde_nummer_fk FOREIGN KEY (kunde_nummer) REFERENCES kugu.kunde (kunde_nummer);

-- Kontrolle
\d+ szdaten.steuerzentrale
