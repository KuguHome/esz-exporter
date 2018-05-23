## Messung

* [Datenbank SQL-Skripte](https://gitlab.kugu-home.com/infrastructure/database)
* [Datenmodell](https://gitlab.kugu-home.com/infrastructure/database/blob/master/2018-01-22%20datenmdodell%20fein%20-%20elementvereinigung,%20kugu-stammdaten%20und%20esz.sql)

Relevante Tabellen:

* Datenbankschema esz.

* Endkunden und Zähler zeigen, ausgehend von SZ:
szdaten.steuerzentrale.sz_nummer -> szdaten.steuerzentrale.kunde_nummer -> kugu.kunde.kunde_nummer -> esz.endkunde.kunde_nummer -> esz.zaehler.kunde_nummer

	-> Zähler angeben über SZ-Nummer, Kundennummer = Endkunde oder direkt über Zähler-Nummer

	select * from szdaten.steuerzentrale join kugu.kunde using (kunde_nummer) join esz.endkunde using (kunde_nummer);

	-> Kunde_Nummer

* Alle Zähler eines Endkunden ausgehend von kunde_nummer:

	select * from esz.zaehler where kunde_nummer = 1;

* Wichtiger Bezug: esz.zaehler.vereinelem_nummer -> szdaten.vereinigung_element.vereinelem_nummer

* Wertvereinigung eines Elements berechnen (vollständige Vereinigung nicht notwendig; letzter Abschnitt reicht):

	alle Elemente aller Abschnitte dieser Vereinigung:
	
	select * from szdaten.vereinigung_element where vereinelem_nummer = 1 order by vereinelem_von_datum ASC;

	zuständiges Element finden:

	select * from szdaten.vereinigung_element where vereinelem_nummer = 1 and vereinelem_bis_datum IS NULL;
	-> elem_nummer
	
	Wert zur letzten Mitternacht finden:
	(Werte sind nie genau um 00:00:00 wegen Millisekunden-Abweichung, aber in real auch nicht auf Millisekunden genau, sondern +1 bis +2 Sekunden - und vor allem nicht in der UTC-Zeitzone sondern in der Deutschland-Zeitzone.)
	
	select * from szdaten.wert_num where elem_nummer = 530;
	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' from szdaten.wert_num where elem_nummer = 530;
		
	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' from szdaten.wert_num where elem_nummer = 530 and EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 and EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin) = 0;

	(Stunden- und Minuten-Werte anzeigen:)

	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') as stunde, EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') as minute from szdaten.wert_num where elem_nummer = 530;

	(darauf selektieren:)

	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt from szdaten.wert_num where elem_nummer = 530 and EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 and EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0;

	-> kann mehrere Wert finden -> den, der am nähesten an Mitternacht dran ist:
	
	(Datumspalte dazunehmen für Gruppierung):
	
	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer IN (308, 467, 530) and EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 and EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 order by wert_zeitpunkt asc;

	gibt mehr als 1 Eintrag:
	
         3 |         530 |   23.400 | 2017-12-30 00:00:02 | 2017-12-30
         3 |         530 |   23.300 | 2017-12-30 00:00:38 | 2017-12-30


	(Normale Gruppierung funktioniert hier nicht, weil nicht nach wert_num gruppiert wird -> Lösung mit Rang-Funktion)
	select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt ASC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from 
		(
		select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer IN (308, 467, 530) and EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 and EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0
		) as werte_mitternacht
		
	(jetzt diejenige herausnehmen, die das Minimum sind = Rang 1 haben)
	
	select sz_nummer, elem_nummer, wert_num, datum 
	from (
		select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt ASC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from 
			(
			select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer = 530 and EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 and EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0
			) as werte_mitternacht
		) werte_mitternacht2
	where wert_zeitpunkt_reihenfolge = 1;
	
	*WICHTIG* Sonderfall am Anfang einer Vereinigungs-Abschnitt ist kein Wert für den Tag eingetragen -> muss in vorherigem Vereinigungs-Abschnitt nachschauen.
	(Kann in der Anwendung gemacht werden, oder hier mittels Oder-Bedingung oder IN auf elem_nummer:)
	
	select sz_nummer, elem_nummer, wert_num, datum 
	from (
		select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt ASC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from 
			(
			select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer IN (308, 467, 530) and EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 and EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0
			) as werte_mitternacht
		) werte_mitternacht2
	where wert_zeitpunkt_reihenfolge = 1;

	-> Liste aller mitternachtsnächsten Zählerwerte :-)
	
	*DONE* Problem: Zählerwerte sind nicht monoton steigend am Gesamtkanal. Richtiges Element bei Zähler/Vereinigung 1?

	Roh-Werte aus Datenbank:
         3 |         530 |   21.700 | 2018-01-23 19:38:54
         3 |         530 |   21.900 | 2018-01-23 20:38:57
         3 |         530 |   22.000 | 2018-01-23 20:58:12
         3 |         530 |   22.100 | 2018-01-23 21:26:58
         3 |         530 |   22.200 | 2018-01-23 22:39:14
         3 |         530 |   22.100 | 2018-01-24 00:39:22
         3 |         530 |   22.000 | 2018-01-24 01:28:41
         3 |         530 |   21.800 | 2018-01-24 02:30:54
         3 |         530 |   21.700 | 2018-01-24 03:01:07
         3 |         530 |   21.900 | 2018-01-24 05:20:36
         3 |         530 |   22.000 | 2018-01-24 05:39:45
         3 |         530 |   22.300 | 2018-01-24 06:39:49
         3 |         530 |   22.500 | 2018-01-24 07:39:52
         
	 *DONE* Teilweise keine Mitternachtswerte -> nur die normalen Abfragewerte um z.B. 00:39 -> Beschränkung auf Minute 0 lockern auf stattdessen Stunde 0.

	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt, datum 
	from (
		select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt ASC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from 
			(
			select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer IN (308, 467, 530)
			) as werte_mitternacht
		) werte_mitternacht2
	where wert_zeitpunkt_reihenfolge = 1;

	-> nicht ganz zufriedenstellend. Noch immer Sprünge. Müssen Zähler1+Zähler2 als Gesamtwert zusammenzählen.
	
	Wert um Mitternacht ist nicht zuverlässig -> letzten Wert des Tages nehmen = mit höchter Zeit je Tag bzw. höchstem Rang.

	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt, datum 
	from (
		select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt DESC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from 
			(
			select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer IN (308, 467, 530)
			) as werte_mitternacht
		) werte_mitternacht2
	where wert_zeitpunkt_reihenfolge = 1;

	*DONE* Sonderfall ausgetauschte Elemente können noch immer störende Werte produzieren, z.B. siehe hier:
	
	select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt, datum 
	from (
		select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt ASC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from 
			(
			select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer IN (290, 446)
			) as werte_mitternacht
		) werte_mitternacht2
	where wert_zeitpunkt_reihenfolge = 1;
	
	-> muss in Applikation mit UNION gemacht werden und die Werte beschränkt auf den Gültigkeitszeitraum:

	select sz_nummer, elem_nummer, wert_num, datum 
	from (
		select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt ASC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from 
			(
			select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt, to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum from szdaten.wert_num where elem_nummer = 530 and to_date(wert_zeitpunkt) between [...] and [...] and EXTRACT(HOUR FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0 and EXTRACT(MINUTE FROM wert_zeitpunkt AT TIME ZONE 'Europe/Berlin') = 0
			) as werte_mitternacht
		) werte_mitternacht2
	where wert_zeitpunkt_reihenfolge = 1;

* letzten Wert eintragen in esz.messung mit Schlüssel kunde_nummer, zaehler_nummer und mess_nummer mit Daten in Feldern mess_datum und mess_zaehlerstand

	-- mess_nummer wird automatisch generiert aus Sequenz esz.messung_mess_nummer_seq
	insert into esz.messung (kunde_nummer, zaehler_nummer, mess_datum, mess_zaehlerstand) values (..........);
