package main

import (
	"fmt"
	"log/syslog"
	"os"
	"time"

	"database/sql"

	pq "github.com/lib/pq"
)

const (
	// Datenbankverbindungs-Parameter
	dbAdresse = "localhost" //TODO für Entwickler
	//dbAdresse   = "kugu-analyse.c9xiiot3ga8j.eu-central-1.rds.amazonaws.com"
	dbBenutzer  = "erohlicek" //TODO eigenen Benutzer esz-export erstellen
	dbDatenbank = "analyse1"
	dbWartezeit = 5 // Sekunden
	// Höchstdauer einer Operation
	maxTime = 5 * time.Minute
	// für Zeitformatierung
	isoZeitformat  = "2006-01-02T15:04:05-07:00"
	isoDatumformat = "2006-01-02"
	// Protokollierung
	logName = "esz-exporter"
)

var (
	// Datenbankverbindung
	dbPasswort string
	// Zeitzone für Zeitselektionen betreffend Mitternacht
	zeitzone, _ = time.LoadLocation("Europe/Berlin")
	// Entwickler-Modus
	debug = false
	// weitere Variablen
	log *syslog.Writer
	// Operationsmodus
	modus = "messung"
)

func main() {
	// mit Systemprotokoll verbinden
	protokollVerbinden()

	// Passwort einlesen
	// mehrere Möglichkeiten: https://stackoverflow.com/questions/38094555/golang-read-os-stdin-input-but-dont-echo-it
	// mehrere Möglichkeiten: https://stackoverflow.com/questions/2137357/getpasswd-functionality-in-go
	fmt.Print("Passwort für Datenbankzugang: ")
	/*
		dbPasswortBytes, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Printf("FEHLER beim Einlesen des Passworts: %s\n", err)
			os.Exit(1)
		}
		dbPasswort = string(dbPasswortBytes)
	*/
	//TODO
	dbPasswort = "..."
	//fmt.Printf("Passwort = >%s<\n", dbPasswort)
	//fmt.Println("")

	// Modus setzen
	modus = "messung"

	// Entwickler-/Test-Modus
	if len(os.Args) == 1+1 && os.Args[1] == "-debug" {
		log.Debug(fmt.Sprintf("[%s] Starte im Entwicklermodus.", modus))
		debug = true
	}

	// Start protokollieren
	log.Info(fmt.Sprintf("[%s] Programmstart.", modus))

	// Generelle Zeitgrenze einstellen
	erledigt := make(chan error, 1)

	// Messung durchführen für alle Zähler
	go messungDurchführen(time.Now().In(zeitzone), erledigt)

	// Zeitablauf oder ordentlicher Status
	select {
	case <-time.After(maxTime):
		log.Warning(fmt.Sprintf("[%s] FEHLER: Zeitüberschreitung für Operation - abgebrochen.", modus))
		fmt.Println("FEHLER")
		log.Close()
		os.Exit(2)
	case err := <-erledigt:
		// Durchgelaufen, auf Fehler überprüfen
		if err != nil {
			log.Err(fmt.Sprintf("[%s] FEHLER: Operation abgebrochen: %s", modus, err.Error()))
			fmt.Println("FEHLER")
			log.Close()
			os.Exit(3)
		}

		// Erfolgsmeldung
		fmt.Println("OK")

		// Erfolg protokollieren
		log.Info(fmt.Sprintf("[%s] OK: Operation erfolgreich.", modus))
	}

	// Ende protokollieren
	log.Info("Programmende.")
	log.Close()
}

// Hauptfunktion
func messungDurchführen(datum time.Time, erledigt chan<- error) {
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

	//TODO für alle Zähler durchführen
	const zählerNummer = 1      // Licht Büro 1
	const vereinigungNummer = 1 // Licht Büro 1

	// Datenbankverbindung aufbauen
	// Dokumentation @ https://github.com/golang/go/wiki/SQLInterface
	// NOTE: gorm hat nicht funktioniert, db.Raw().Scan().Error hat immer leeres Ergebnis geliefert
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
		if err := rows.Err(); err != nil {
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

	//TODO Messwerte in esz.messung eintragen

	//TODO Unterscheidung Modus Aufholen vgl. mit datumbezogenen Messung

	// Erfolg melden (nil = kein Fehler)
	erledigt <- nil
	//erledigt <- fmt.Errorf("Versionsmarkierung unbekannt: %s", version)
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

func protokollVerbinden() {
	var err error
	log, err = syslog.New(syslog.LOG_ERR|syslog.LOG_DAEMON, logName)
	if err != nil {
		fmt.Println("FEHLERINT")
		os.Exit(1)
	}
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
