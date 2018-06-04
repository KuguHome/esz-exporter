package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Messung durchführen bzw. aufholen
func messungDurchführen(datum time.Time, aufholen bool, erledigt chan<- error) {
	// Ausgabe
	//TODO if -debug
	/*
		for name, element := range daten.ZahlenElemente {
			fmt.Println("Element erhalten: " + name)
			for _, datum := range element {
				fmt.Printf("\t%20s  %8.2f\n", datum.Zeit.Format(time.RFC822), datum.Wert)
			}
		}
	*/

	///TODO für alle Endkunden durchführen

	///TODO für alle Zähler durchführen
	///TODO Zähler -> Vereinigungselement-Nummer holen

	// Datenbankverbindung aufbauen
	// Dokumentation @ https://github.com/golang/go/wiki/SQLInterface
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s sslmode=disable connect_timeout=%d user=%s password=%s dbname=%s", dbAdresse, dbWartezeit, dbBenutzer, dbPasswort, dbDatenbank))
	if err != nil {
		erledigt <- fmt.Errorf("[%s] Datenbankverbindung fehlgeschlagen: %s", modus, err)
		return
	}
	defer db.Close()
	log.Debug(fmt.Sprintf("[%s] Datenbankverbindung erfolgreich", modus))

	// TODO Zähler -> elemverein_nummer

	// Wertvereinigung berechnen:

	// zuständige Elemente finden mit Gültigkeitsbereich
	log.Debug(fmt.Sprintf("[%s] Zähler %d: Zuständige Elemente finden", modus, zählerNummer))
	zuständigeElemente := []ZuständigesElement{}
	rows, err := db.Query("select sz_nummer, elem_nummer, vereinelem_von_datum, vereinelem_bis_datum from szdaten.vereinigung_element where vereinelem_nummer = $1 order by vereinelem_von_datum ASC", vereinigungNummer)
	if err != nil {
		//TODO
		panic(err)
	}
	var szNummer, zuständigesElementNummer int
	var gültigVon time.Time
	var gültigBis pq.NullTime
	for rows.Next() {
		if err = rows.Scan(&szNummer, &zuständigesElementNummer, &gültigVon, &gültigBis); err != nil {
			erledigt <- fmt.Errorf("Zähler %d: Zuständiges Element einlesen: %s", zählerNummer, err)
			//TODO Logeintrag
			return
		}
		//TODO if debug -> Eintrag ausgeben
		zuständigeElemente = append(zuständigeElemente, ZuständigesElement{
			Nummer:    zuständigesElementNummer,
			GültigVon: gültigVon,
			GültigBis: NullTimeToTime(gültigBis),
		})
	}
	if err = rows.Err(); err != nil {
		erledigt <- fmt.Errorf("Zähler %d: Zuständige Elemente einlesen: %s", zählerNummer, err)
		//TODO Logeintrag
		return
	}
	// Erfolg
	log.Info(fmt.Sprintf("[%s] Zuständige Elemente gefunden: %v", modus, zuständigeElemente))
	rows.Close()

	//TODO if len == 0

	// Wertvereinigung berechnen
	//TODO falls notwendig für Sonderfälle

	// direkt die Zählerwerte berechnen
	zählerWerte := []ZählerWert{}
	for index, zuständigesElement := range zuständigeElemente {
		log.Debug(fmt.Sprintf("[%s] Zähler %d: Zählerwerte berechnen (%d von %d)", modus, zählerNummer, index+1, len(zuständigeElemente)))
		sqlStr := fmt.Sprintf(
			`
			select sz_nummer, elem_nummer, wert_num, wert_zeitpunkt, datum
			from (
				select RANK() OVER(PARTITION BY datum ORDER BY wert_zeitpunkt ASC) as wert_zeitpunkt_reihenfolge, werte_mitternacht.* from
					(
					select
						sz_nummer,
						elem_nummer,
						wert_num,
						wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' as wert_zeitpunkt,
						to_char(wert_zeitpunkt AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD') as datum
					from szdaten.wert_num
					where
						elem_nummer = $1 and
						wert_zeitpunkt AT TIME ZONE 'Europe/Berlin' %s
					) as werte_mitternacht
				) werte_mitternacht2
			where wert_zeitpunkt_reihenfolge = 1;
			`,
			TimeToCondition(zuständigesElement))
		var rows *sql.Rows
		if zuständigesElement.GültigBis.IsZero() {
			rows, err = db.Query(sqlStr, zuständigesElement.Nummer, zuständigesElement.GültigVon.Format(isoDatumformat))
		} else {
			rows, err = db.Query(sqlStr, zuständigesElement.Nummer, zuständigesElement.GültigVon.Format(isoDatumformat), zuständigesElement.GültigBis.Format(isoDatumformat))
		}
		if err != nil {
			//TODO
			panic(err)
		}
		var szNummer, elementNummer int
		var wert float64
		var zeitpunkt time.Time
		var datum string
		for rows.Next() {
			err = rows.Scan(&szNummer, &elementNummer, &wert, &zeitpunkt, &datum)
			if err != nil {
				erledigt <- fmt.Errorf("Zähler %d: Zählerwerte berechnen fehlgeschlagen: %s", zählerNummer, err)
				//TODO Logeintrag
				return
			}
			// Details ausgeben
			//TODO if debug
			fmt.Printf("Zähler %d: Zählerwert: Element %4d Zeitpunkt %15s Datum %10s Wert %6.2f\n", zählerNummer, elementNummer, zeitpunkt.Format(isoZeitformat), datum, wert)
			// Eintrag speichern
			//TODO Unterschied Datum und Zeitpunkt hier?
			zählerWerte = append(zählerWerte, ZählerWert{
				Datum: zeitpunkt,
				Wert:  wert,
			})
		}
		if err = rows.Err(); err != nil {
			erledigt <- fmt.Errorf("Zähler %d: Zählerwerte berechnen fehlgeschlagen: %s", zählerNummer, err)
			//TODO Logeintrag
			return
		}
	}
	// Erfolg
	log.Info(fmt.Sprintf("[%s] Zählerwerte berechnet, Anzahl: %d", modus, len(zählerWerte)))
	//TODO if debug
	/*
		for _, zählerwert := range zählerWerte {
			fmt.Printf("Datum %10s Wert %6.2f\n", zählerwert.Datum.Format(isoDatumformat), zählerwert.Wert)
		}
	*/

	// Kontrollen
	for _, zählerWert := range zählerWerte {
		// sollte positiv sein
		if zählerWert.Wert < 0 {
			erledigt <- fmt.Errorf("Zähler %d: Zählerwert für Datum %s negativ: %.3f", zählerNummer, zählerWert.Datum.Format(isoDatumformat), zählerWert.Wert)
			return
		}
	}

	// Messwerte in esz.messung eintragen
	if !aufholen {
		// alle Einträge filtern auf das gegebene Datum
		indexGefunden := -1
		for index, zählerWert := range zählerWerte {
			if zählerWert.Datum.In(zeitzone).Year() == datum.Year() && zählerWert.Datum.In(zeitzone).Month() == datum.Month() && zählerWert.Datum.In(zeitzone).Day() == datum.Day() {
				if indexGefunden != -1 {
					// mehr als einen für diesen Tag gefunden = Fehlerfall
					erledigt <- fmt.Errorf("Mehr als einen Zählerwert für Datum %s gefunden", datum.Format(isoDatumformat))
					return
				}
				indexGefunden = index
			}
		}
		aufheben := zählerWerte[indexGefunden]
		zählerWerte = []ZählerWert{aufheben}
	} else {
		// Kontrolle auf Doppeleinträge
		var jahr, tag int
		var monat time.Month
		for index, zählerWert := range zählerWerte {
			// mit .In(zeitzone) ergeben sich Doppelwerte; geht auch nur um Datum
			if zählerWert.Datum.Year() == jahr && zählerWert.Datum.Month() == monat && zählerWert.Datum.Day() == tag {
				// hat sich nicht verändert seit letztem Eintrag also doppelt vorhanden
				erledigt <- fmt.Errorf("Mehr als einen Zählerwert für Datum %s gefunden (Index %d)", zählerWert.Datum.Format(isoDatumformat), index)
				return
			}
			jahr = zählerWert.Datum.Year()
			monat = zählerWert.Datum.Month()
			tag = zählerWert.Datum.Day()
		}
	}

	// Messwerte speichern
	var anzZeilen int64
	var ergebnis sql.Result
	for _, wert := range zählerWerte {
		// Eintrag anlegen
		ergebnis, err = db.Exec("insert into esz.messung (kunde_nummer, zaehler_nummer, mess_datum, mess_zaehlerstand) values ($1, $2, $3, $4)",
			kundeNummer, zählerNummer, wert.Datum, wert.Wert)
		if err != nil {
			erledigt <- fmt.Errorf("Zähler %d: Messung %v anlegen fehlgeschlagen: %s", zählerNummer, wert, err)
			return
		}
		anzZeilen, err = ergebnis.RowsAffected()
		if err != nil {
			erledigt <- fmt.Errorf("Zähler %d: Messung %v anlegen fehlgeschlagen: %s", zählerNummer, wert, err)
			return
		}
		if anzZeilen != 1 {
			erledigt <- fmt.Errorf("Zähler %d: Messung %v anlegen: RowsAffected() ist != 1: %d", zählerNummer, wert, anzZeilen)
			return
		}
		// Erfolg
		log.Debug(fmt.Sprintf("[%s] Zähler %d: Messung %v erfolgreich angelegt", modus, zählerNummer, wert))
	}

	// Erfolg melden
	log.Info(fmt.Sprintf("[%s] Messungen für Zähler %d: %d Messungen eingetragen", modus, zählerNummer, len(zählerWerte)))

	// Erfolg melden
	erledigt <- nil
}

// ZuständigesElement ist ein für einen Zeitabschnitt einer Vereinigungsreihe zuständiges Element
type ZuständigesElement struct {
	Nummer    int
	GültigVon time.Time
	GültigBis time.Time
}

// ZählerWert ist ein Ergebniszählerwert, wie er dann in der Tabelle esz.messung abgelegt werden kann
type ZählerWert struct {
	Datum time.Time
	Wert  float64
}

func (e ZuständigesElement) String() string {
	return fmt.Sprintf("{Nr. %d von %s bis %s}", e.Nummer, e.GültigVon.Format(isoDatumformat), e.GültigBis.Format(isoDatumformat))
}

// NullTimeToTime konvertiert eine möglicherweise leere PostgreSQL-Zeitangabe in eine leere Go-Zeitangabe
func NullTimeToTime(zeit pq.NullTime) time.Time {
	if zeit.Valid {
		return zeit.Time
	}
	return time.Time{} // .IsZero() == true
}

// TimeToCondition konvertiert eine von-bis-Zeitbereichsangabe in eine offene oder geschlossene SQL-Zeitbedingung
func TimeToCondition(elem ZuständigesElement) string {
	if !elem.GültigBis.IsZero() {
		// beschränkt gültiger Vereinigungseintrag
		return "BETWEEN $2 and $3"
	}
	// rechts offen = letzter Eintrag in Elementvereinigung
	return "> $2"
}
