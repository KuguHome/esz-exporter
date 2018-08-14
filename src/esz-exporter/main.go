package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/syslog"
	"os"
	"syscall"
	"time"

	"golang.org/x/crypto/ssh/terminal"
)

const (
	// Datenbankverbindungs-Parameter
	///TODO eigenen Benutzer esz-export erstellen
	dbDatenbank = "analyse1"
	dbWartezeit = 5 // Sekunden
	// Höchstdauer einer Operation
	maxTime = 5 * time.Minute
	// für Zeitformatierung
	isoZeitformat           = "2006-01-02T15:04:05-07:00"
	isoZeitformatFreundlich = "2006-01-02T15:04:05"
	isoDatumformat          = "2006-01-02"
	// Protokollierung
	logName = "esz-exporter"
	// statische Werte in den Exportdaten
	antragsteller     = "024" // nicht 2017024
	abfragehäufigkeit = "4"
	csvTrennzeichen   = ';'
)

var (
	// Datenbankverbindung
	dbAdresse  string
	dbBenutzer string
	dbPasswort string
	// Zeitzone für Zeitselektionen betreffend Mitternacht
	zeitzone, _ = time.LoadLocation("Europe/Berlin")
	// Entwickler-Modus
	debug = false
	// weitere Variablen
	log *syslog.Writer
	// Programmparameter
	modus             = "messung"
	kundeNummer       = 1
	jahr              = 2018
	monat             = 5
	zählerNummer      = 7
	vereinigungNummer = 39
	ausgabePfad       = "/dev/shm"
	jetzt             = time.Now().In(zeitzone)
)

func main() {
	// mit Systemprotokoll verbinden
	protokollVerbinden()

	// Parameter einlesen
	var help, debug bool
	flag.BoolVar(&help, "h", false, "Programmhilfe und Parameter ausgeben")
	flag.BoolVar(&debug, "debug", false, "detaillierte Programmausgabe")
	flag.StringVar(&modus, "modus", "messung", "Betriebsmodus umschalten: messung, aufholen, zählersumme, exportbefüllen, exportieren")
	flag.IntVar(&kundeNummer, "kunde", 1, "Endkunden auswählen")
	flag.IntVar(&jahr, "jahr", 2018, "Export: Jahr auswählen")
	flag.IntVar(&monat, "monat", 5, "Export: Monat auswählen")
	flag.IntVar(&zählerNummer, "zähler", 7, "zu bearbeitende Zählernummer")
	flag.IntVar(&vereinigungNummer, "vereinigung", 39, "zu bearbeitendes Vereinigungselement")
	flag.StringVar(&ausgabePfad, "pfad", "/dev/shm", "Verzeichnispfad für Ausgabedateien")
	flag.StringVar(&dbAdresse, "dbadresse", "kugu-analyse.c9xiiot3ga8j.eu-central-1.rds.amazonaws.com", "Adresse bzw. Hostname der Analyse-Datenbank")
	flag.StringVar(&dbBenutzer, "dbbenutzer", "erohlicek", "Datenbank-Benutzername")
	flag.StringVar(&dbPasswort, "dbpasswort", "", "Passwort als Parameter übergeben statt sichere Eingabeaufforderung (Achtung Sicherheitsrisiko; nur für Entwickler-Testlauf)")
	flag.Parse()
	if help {
		printUsage()
	}

	// Parameterkontrollen
	if modus == "" {
		fmt.Println("FEHLER: kein Betriebsmodus angegeben") //TODO Protokollierung ins Systemprotokoll
		os.Exit(1)
	}
	if flag.NArg() != 0 {
		fmt.Println("FEHLER: unerwartete freie Argumente angegeben:", flag.Args()) //TODO auch ins Systemprotokoll
		os.Exit(1)
	}

	// Passwort einlesen
	if dbPasswort == "" {
		// mehrere Möglichkeiten: https://stackoverflow.com/questions/38094555/golang-read-os-stdin-input-but-dont-echo-it
		// mehrere Möglichkeiten: https://stackoverflow.com/questions/2137357/getpasswd-functionality-in-go
		fmt.Printf("Datenbankpasswort für %s: ", dbBenutzer)
		dbPasswortBytes, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Printf("FEHLER beim Einlesen des Passworts: %s\n", err)
			os.Exit(1)
		}
		dbPasswort = string(dbPasswortBytes)
		//fmt.Printf("Passwort = >%s<\n", dbPasswort)
		//fmt.Println("")
	}

	// Entwickler-/Test-Modus
	if debug {
		log.Debug(fmt.Sprintf("[%s] Starte im Entwicklermodus.", modus))
	}

	// Start protokollieren
	log.Info(fmt.Sprintf("[%s] Programmstart.", modus))

	// Generelle Zeitgrenze einstellen
	erledigt := make(chan error, 1)

	switch modus {
	case "messung":
		// Messung durchführen für alle Zähler
		go messungDurchführen(jetzt, false, erledigt)
	case "aufholen":
		// nur einmal auszuführen
		go messungDurchführen(jetzt, true, erledigt)
	case "exportbefüllen":
		// Monatsexport: vor Exportieren durchführen
		go exportBefüllenDurchführen(erledigt)
	case "exportieren":
		// Export-Einträge aus Export-Tabelle holen und in Datei exportieren
		go exportDateiDurchführen(ausgabePfad, erledigt)
	case "zählersumme":
		// Zählersumme jährlich -> Zählersumme befüllen
		go zählerSummeErstellen(time.Now().In(zeitzone), erledigt)
	default:
		fmt.Println("FEHLER: ungültiger Betriebsmodus:", modus) //TODO auch ins Systemprotokoll eintragen
		printUsage()
		os.Exit(1)
	}

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

func protokollVerbinden() {
	var err error
	log, err = syslog.New(syslog.LOG_ERR|syslog.LOG_DAEMON, logName)
	if err != nil {
		fmt.Println("FEHLERINT")
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Aufruf:", os.Args[0], "-modus [messung|aufholen|exportbefüllen|exportieren|zählersumme] [weitere-parameter...]")
	flag.PrintDefaults()
	os.Exit(1)
}

func verbindeDatenbank() (db *sql.DB, err error) {
	// Datenbankverbindung aufbauen
	// Dokumentation @ https://github.com/golang/go/wiki/SQLInterface
	db, err = sql.Open("postgres", fmt.Sprintf("host=%s sslmode=disable connect_timeout=%d user=%s password=%s dbname=%s", dbAdresse, dbWartezeit, dbBenutzer, dbPasswort, dbDatenbank))
	if err != nil {
		return nil, fmt.Errorf("[%s] Datenbankverbindung fehlgeschlagen: %s", modus, err)
	}
	log.Debug(fmt.Sprintf("[%s] Datenbankverbindung erfolgreich", modus))

	return db, err
}

func trenneDatenbank(db *sql.DB) {
	db.Close()
}
