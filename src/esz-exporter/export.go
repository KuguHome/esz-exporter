package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	timeutil "github.com/jinzhu/now"
)

func exportBefüllenDurchführen(erledigt chan<- error) {
	// Exporttabelle befüllen
	//TODO Funktion exporttabelleBefüllen() ev. hier hereinziehen
	if err := exporttabelleBefüllen(kundeNummer, jahr, monat); err != nil {
		erledigt <- fmt.Errorf("Konnte Export-Tabelle nicht befüllen: %s", err)
		return
	}
	log.Info(fmt.Sprintf("[%s] Export-Tabelle erfolgreich befüllt", modus))

	erledigt <- nil
	return
}

func exportDateiDurchführen(ausgabePfad string, erledigt chan<- error) {
	log.Info(fmt.Sprintf("[%s] Starte mit Exportdatei-Generierung für Endkunde %d", modus, kundeNummer))
	// Testdaten
	//var daten = [][]string{{"Spalte1", "Spalte2"}, {"Wert1.1", "Wert1.2"}, {"Wert2.1", "Wert2.2"}}

	// Realdaten aus Export-Tabelle
	log.Info(fmt.Sprintf("[%s] Hole Daten für Endkunde %d aus Export-Tabelle", modus, kundeNummer))
	exportDaten, err := holeExporttabelle(0) ///TODO bis zu welcher Messung wurde schon exportiert?
	if err != nil {
		erledigt <- fmt.Errorf("Konnte Daten aus Export-Tabelle nicht holen: %s", err)
		return
	}
	/*
		csvDaten := [][]string{}
		for index, eintrag := range exportDaten {
			fmt.Printf("Exportdatensatz %d: %v\n", index+1, eintrag)
			// in CSV-Einträge konvertieren
			csvDaten = append(csvDaten, eintrag.String())
		}
	*/

	// eine Messung je CSV-Datei
	log.Info(fmt.Sprintf("[%s] Erzeuge Exportdateien für Endkunde %d", modus, kundeNummer))
	dateipfadeZippen := []string{}
	for index, eintrag := range exportDaten {
		if (index+1)%25 == 0 {
			log.Info(fmt.Sprintf("[%s] Fortschritt: %d von %d", modus, index+1, len(exportDaten)))
		}
		fmt.Printf("Exportdatensatz %d: %v\n", index+1, eintrag)
		// in CSV-Eintrag konvertieren
		csvEintrag := eintrag.String()

		// Datei anlegen
		dateiPfad := fmt.Sprintf("%s/%s-%d.csv", ausgabePfad, antragsteller, eintrag.MessungNummer)
		datei, err := os.Create(dateiPfad)
		if err != nil {
			erledigt <- fmt.Errorf("Konnte Datei nicht anlegen: %s", err)
			return
		}
		//defer datei.Close()

		//TODO Ausgabe je Messung in einzelne Datei, Dateiname = "%s-%d", antragsteller, messungNummer
		//TODO Liste der erstellten Dateien erstellen. Dateiname nicht spezifiziert, aber z.B. "%s 2018-01.zip"

		//TODO Anzahl der CSV − Dateien im Monat = Anzahl der Zähler (bei allen Endkunden) ∗ Anzahl der Tage im Monat

		// CSV-Schreiber anlegen
		//csv := csv.NewWriter(datei)
		buf := &bytes.Buffer{}
		csv := csv.NewWriter(buf)
		csv.Comma = csvTrennzeichen
		//defer csv.Flush()

		// alle Einträge als CSV schreiben
		//for _, eintrag := range csvDaten {
		err = csv.Write(csvEintrag)
		if err != nil {
			erledigt <- fmt.Errorf("Konnte nicht in CSV-Schreiber schreiben: %s", err)
			return
		}
		//}

		// CSV-Ausgabe abschließen
		csv.Flush()
		_, err = datei.Write(bytes.TrimSpace(buf.Bytes()))
		if err != nil {
			erledigt <- fmt.Errorf("Konnte nicht in Datei schreiben: %s", err)
			return
		}
		datei.Close()

		// Dateiname merken
		dateipfadeZippen = append(dateipfadeZippen, dateiPfad)
	}
	log.Info(fmt.Sprintf("[%s] Fortschritt: %d von %d - fertig.", modus, len(exportDaten), len(exportDaten)))

	// ZIP-Kompression der CSV-Datei
	log.Info(fmt.Sprintf("[%s] Erstelle Abgabedatei für Endkunde %d", modus, kundeNummer))
	//if err := zipKompression(ausgabePfad+".zip", []string{ausgabePfad + ".csv"}); err != nil {
	zipArchivpfad := fmt.Sprintf("%s/esz-export-kugu-%s.zip", ausgabePfad, jetzt.Format(isoZeitformatFreundlich))
	if err := zipKompression(zipArchivpfad, dateipfadeZippen); err != nil {
		erledigt <- fmt.Errorf("Konnte ZIP-Archiv nicht erstellen: %s", err)
		return
	}
	log.Debug(fmt.Sprintf("[%s] Abgabepaket erstellt unter %s", modus, zipArchivpfad))

	erledigt <- nil
	return
}

func zipKompression(ausgabePfad string, dateiPfade []string) error {
	zipDatei, err := os.Create(ausgabePfad)
	if err != nil {
		return err
	}
	defer zipDatei.Close()

	zipWriter := zip.NewWriter(zipDatei)
	defer zipWriter.Close()

	// alle Dateien hinzufügen
	for _, dateiPfad := range dateiPfade {
		dateiHinzufügen, err := os.Open(dateiPfad)
		if err != nil {
			return err
		}
		defer dateiHinzufügen.Close()

		// Get the file information
		dateiInfo, err := dateiHinzufügen.Stat()
		if err != nil {
			return err
		}

		zipEintrag, err := zip.FileInfoHeader(dateiInfo)
		if err != nil {
			return err
		}

		// stärkere Kompression verwenden, siehe http://golang.org/pkg/archive/zip/#pkg-constants
		zipEintrag.Method = zip.Deflate

		zipDatenstrom, err := zipWriter.CreateHeader(zipEintrag)
		if err != nil {
			return err
		}
		_, err = io.Copy(zipDatenstrom, dateiHinzufügen)
		if err != nil {
			return err
		}
	}

	return nil
}

// ExportDatensatz enthält einen Datensatz aus der Tabelle esz.export; Felder siehe ESZ-Datenmodell SQL-Skript
type ExportDatensatz struct {
	AntragstellerNummer                 int       // Applikationskonstante
	MessungNummer                       int       // messung.mess_nummer
	KundeNummer                         int       // endkunde.kunde_nummer
	EndkundeSystemArt                   int       // ndkunde.endku_system_art
	EndkundeGröße                       int       // endkunde.endku_groesse
	EndkundeBranche                     int       // endkunde.endku_branche
	EndkundePostleitzahl                int       // endkunde.endku_postleitzahl
	EndkundeBewohnerAnzahl              int       // endkunde.endku_bewohner_anzahl
	EndkundeGebäudeTyp                  int       // endkunde.endku_gebaeude_typ
	EndkundeBaualterklasse              int       // endkunde.endku_baualterklasse
	EndkundeSanierungEnergieLetzte      int       // endkunde.endku_sanierung_energ_letzte
	EndkundeFlächeGesamt                int       // endkunde.endku_flaeche_gesamt
	MessungDatum                        time.Time // messung.mess_datum (als Ganzzahl 8 im Format JJJJMMTT)
	ZählerID                            int       // zaehler.zaehler_id
	ZählerIDÜbergeordnet                int       // zaehler.zaehler_id_uebergeordnet
	ZählerArt                           int       // zaehler.zaehler_art
	ZählerGeräteArt                     int       // zaehler.zaehler_geraete_art
	ZählerGeräteSonstiges               string    // zaehler.zaehler_geraete_sonstiges_freitext
	ZählerTyp                           int       // zaehler.zaehler_typ
	ZählerEnergieträger                 int       // zaehler.zaehler_energietraeger
	MessungZählerstand                  float64   // messung.mess_zaehlerstand (ev. gleich als Zeichenkette aus DB rausholen)
	OptimierungMaßnahme                 int       // optimierung.opt_massnahme
	OptimierungSonstiges                string    // optimierung.opt_sonstiges_freitext
	EndkundeSMGStatus                   int       // endkunde.endku_smg_status
	ZählersummeSummeEingespart          float64   // zaehlersumme.zaehlersum_summe_eingespart
	ZählersummeSummeEingespartBereinigt float64   // zaehlersumme.zaehlersum_summe_eingespart_bereinigt
	Abfragehäufigkeit                   int       // Applikationskonstante
	Klimafaktor                         float64   // klimafaktor.klimafak_faktor
	/*
	   export_einfluss1_wert   SMALLINT null,          --entfällt (leer im Export)
	   export_einfluss1_art    SMALLINT null,          --entfällt
	   export_einfluss2_wert   SMALLINT null,          --entfällt
	   export_einfluss2_art    SMALLINT null,          --entfällt
	   export_einfluss3_wert   SMALLINT null,          --entfällt
	   export_einfluss3_art    SMALLINT null,          --entfällt
	   export_einfluss4_wert   SMALLINT null,          --entfällt
	   export_einfluss4_art    SMALLINT null,          --entfällt
	   export_nutzen1_wert     SMALLINT null,          --entfällt
	   export_nutzen1_art      SMALLINT null,          --entfällt
	   export_nutzen2_wert     SMALLINT null,          --entfällt
	   export_nutzen2_art      SMALLINT null,          --entfällt
	*/
	ZählersummeSummeBasislinie          float64 // zaehlersumme.zaehlersum_summe_basislinie
	ZählersummeSummeBasislinieBereinigt float64 // zaehlersumme.zaehlersum_summe_basislinie_bereinigt
	/*
	   export_einfluss1_basislinie_gemittelt   SMALLINT null,  --entfällt
	   export_einfluss2_basislinie_gemittelt   SMALLINT null,  --entfällt
	   export_einfluss3_basislinie_gemittelt   SMALLINT null,  --entfällt
	   export_einfluss4_basislinie_gemittelt   SMALLINT null,  --entfällt
	   export_nutzen1_basislinie_gemittelt     SMALLINT null,  --entfällt
	   export_nutzen2_basislinie_gemittelt     SMALLINT null,  --entfällt
	*/
}

const eszDatumformat = "20060102"

func (ds ExportDatensatz) String() []string {
	return []string{
		fmt.Sprintf("%03d", ds.AntragstellerNummer),
		strconv.Itoa(ds.MessungNummer),
		strconv.Itoa(ds.KundeNummer),
		strconv.Itoa(ds.EndkundeSystemArt),
		strconv.Itoa(ds.EndkundeGröße),
		strconv.Itoa(ds.EndkundeBranche),
		fmt.Sprintf("%05d", ds.EndkundePostleitzahl),
		strconv.Itoa(ds.EndkundeBewohnerAnzahl),
		strconv.Itoa(ds.EndkundeGebäudeTyp),
		strconv.Itoa(ds.EndkundeBaualterklasse),
		strconv.Itoa(ds.EndkundeSanierungEnergieLetzte),
		strconv.Itoa(ds.EndkundeFlächeGesamt),
		ds.MessungDatum.Format(eszDatumformat),
		strconv.Itoa(ds.ZählerID),
		strconv.Itoa(ds.ZählerIDÜbergeordnet),
		strconv.Itoa(ds.ZählerArt),
		strconv.Itoa(ds.ZählerGeräteArt),
		ds.ZählerGeräteSonstiges,
		strconv.Itoa(ds.ZählerTyp),
		strconv.Itoa(ds.ZählerEnergieträger),
		strconv.FormatFloat(math.Round(ds.MessungZählerstand), 'f', 0, 64), // soll kaufmännisch gerundet werden
		fmt.Sprintf("%02d", ds.OptimierungMaßnahme),                        // soll immer zweistllig sein
		ds.OptimierungSonstiges,
		strconv.Itoa(ds.EndkundeSMGStatus),
		strconv.FormatFloat(math.Round(undefiniertZuNull(ds.ZählersummeSummeEingespart)), 'f', 0, 64),          // soll kaufmännisch gerundet werden
		strconv.FormatFloat(math.Round(undefiniertZuNull(ds.ZählersummeSummeEingespartBereinigt)), 'f', 0, 64), // soll kaufmännisch gerundet werden
		strconv.Itoa(ds.Abfragehäufigkeit),
		strconv.FormatFloat(undefiniertZuNull(ds.Klimafaktor), 'f', 3, 64), // RZ(3) = drei Kommastellen
		"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", // entfallende Felder: 4x Einfluss und 2x Nutzen mit jeweils Wert und Art
		strconv.FormatFloat(math.Round(undefiniertZuNull(ds.ZählersummeSummeBasislinie)), 'f', 0, 64),          // soll kaufmännisch gerundet werden
		strconv.FormatFloat(math.Round(undefiniertZuNull(ds.ZählersummeSummeBasislinieBereinigt)), 'f', 0, 64), // soll kaufmännisch gerundet werden
		"0", "0", "0", "0", "0", "0", // entfallende Felder: 4x Einfluss, 2x Nutzen
	}
}

func undefiniertZuNull(wert float64) float64 {
	// undefiniert zu 0 übersetzen
	if wert == -1.0 {
		return 0.0
	}
	// ansonsten normalen Wert zurückliefern
	return wert
}

func exporttabelleBefüllen(kundeNummer int, jahr int, monat int) (err error) {
	// Datenbankverbindung aufbauen
	// Dokumentation @ https://github.com/golang/go/wiki/SQLInterface
	// NOTE: gorm hat nicht funktioniert, db.Raw().Scan().Error hat immer leeres Ergebnis geliefert
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s sslmode=disable connect_timeout=%d user=%s password=%s dbname=%s", dbAdresse, dbWartezeit, dbBenutzer, dbPasswort, dbDatenbank))
	if err != nil {
		return fmt.Errorf("[%s] Datenbankverbindung fehlgeschlagen: %s", modus, err)
	}
	defer db.Close()
	log.Debug(fmt.Sprintf("[%s] Datenbankverbindung erfolgreich", modus))

	einträge, err := db.Query(fmt.Sprintf(`
		select
			'%s' as konfig_antragsteller_nummer,
			mess_nummer,
			esz.endkunde.kunde_nummer,
			endku_system_art,
			endku_groesse,
			endku_branche,
			endku_postleitzahl,
			endku_bewohner_anzahl,
			endku_gebaeude_typ,
			endku_baualterklasse,
			endku_sanierung_energ_letzte,
			endku_flaeche_gesamt,
			mess_datum,
			zaehler_id,
			zaehler_id_uebergeordnet,
			zaehler_art,
			zaehler_geraete_art,
			zaehler_geraete_sonstiges_freitext,
			zaehler_typ,
			zaehler_energietraeger,
			mess_zaehlerstand,
			0 as opt_massnahme,
			'' as opt_sonstiges_freitext,
			endku_smg_status,
			-1 as zaehlersum_summe_eingespart,
			-1 as zaehlersum_summe_eingespart_bereinigt,
			%s as konfig_abfrage_haeufigkeit,
			-1 as klimafak_faktor,
			-- export_einfluss* und export_nutzen* entfällt (leer im Export)
			-1 as zaehlersum_summe_basislinie,
			-1 as zaehlersum_summe_basislinie_bereinigt
			-- export_einfluss*_basislinie_gemittelt und export_nutzen*_basislinie_gemittelt entfällt (leer im Export)
		from
			esz.endkunde
			join esz.zaehler using (kunde_nummer)
			join esz.messung using (zaehler_nummer)
		where
			endkunde.kunde_nummer = $1 and
			EXTRACT(YEAR FROM mess_datum) = $2 and
			EXTRACT(MONTH FROM mess_datum) = $3 and
			vereinelem_nummer > 0
		order by mess_datum ASC;`,
		antragsteller, abfragehäufigkeit),
		kundeNummer, jahr, monat)
	//TODO Join mit Zählersumme-Tabelle -> Werte von dort dazunehmen
	if err != nil {
		return fmt.Errorf("Export-Einträge generieren: %s", err)
	}
	var datenExport []ExportDatensatz
	var letzteMessung int
	for einträge.Next() {
		ds := ExportDatensatz{}
		if err = einträge.Scan(
			&ds.AntragstellerNummer, &ds.MessungNummer, &ds.KundeNummer,
			&ds.EndkundeSystemArt, &ds.EndkundeGröße, &ds.EndkundeBranche, &ds.EndkundePostleitzahl, &ds.EndkundeBewohnerAnzahl,
			&ds.EndkundeGebäudeTyp, &ds.EndkundeBaualterklasse, &ds.EndkundeSanierungEnergieLetzte, &ds.EndkundeFlächeGesamt,
			&ds.MessungDatum, &ds.ZählerID, &ds.ZählerIDÜbergeordnet, &ds.ZählerArt, &ds.ZählerGeräteArt, &ds.ZählerGeräteSonstiges, &ds.ZählerTyp,
			&ds.ZählerEnergieträger, &ds.MessungZählerstand, &ds.OptimierungMaßnahme, &ds.OptimierungSonstiges, &ds.EndkundeSMGStatus,
			&ds.ZählersummeSummeEingespart, &ds.ZählersummeSummeEingespartBereinigt, &ds.Abfragehäufigkeit, &ds.Klimafaktor,
			&ds.ZählersummeSummeBasislinie, &ds.ZählersummeSummeBasislinieBereinigt); err != nil {
			return fmt.Errorf("Exportdatensätze (letzte erfolgreiche Messung: %d) in Datenstruktur einlesen: %s", letzteMessung, err)
		}
		letzteMessung = ds.MessungNummer
		//TODO if debug -> Eintrag ausgeben
		//TODO Zeitangaben die leer sein können? NullTimeToTime(gültigBis),
		datenExport = append(datenExport, ds)
	}
	if err = einträge.Err(); err != nil {
		return fmt.Errorf("Einträge aus Export-Tabelle auslesen: %s", err)
	}

	// Erfolg
	log.Info(fmt.Sprintf("[%s] Vollständige Export-Einträge gefunden: %d", modus, len(datenExport)))
	einträge.Close()

	// Kontrolle auf Anzahl der Einträge

	// Anzahl Tage im Monat berechnen
	monatAnfang := timeutil.Now{Time: time.Date(jahr, time.Month(monat), 1, 0, 0, 0, 0, zeitzone)}
	//monatEnde := now.BeginningOfMonth().AddDate(0, 1, 0).Add(-time.Nanosecond)
	monatEnde := monatAnfang.EndOfMonth()
	anzTage := monatEnde.Day() - monatAnfang.Day() + 1

	// Anzahl Zähler dieses Kunden holen
	anzZähler, err := anzahlZähler(db, kundeNummer)
	//anzZähler = 1 ///TODO
	if err != nil {
		return fmt.Errorf("Export-Einträge generieren: %s", err)
	}

	messungenErwartet := anzTage * anzZähler
	if len(datenExport) != messungenErwartet {
		// Messungen und/oder Zählersummen-Einträge fehlen
		return fmt.Errorf("Export-Einträge generieren: Messungen und/oder Zählersummen-Einträge unvollständig: erwartet %d, vorhanden %d", messungenErwartet, len(datenExport))
	}

	// Export-Tabelle befüllen
	var anzZeilen int64
	var ergebnis sql.Result
	for _, ds := range datenExport {
		// für dieses Datum zuständige Zählersummen-Einträge holen
		// NOTE: könnte auch direkt in Abfrage oben mitgemacht werden.
		_, _, zsBasislinieWert, zsBasislinieFaktor, err := holeZählersummeBasislinie(db, kundeNummer, zählerNummer)
		if err != nil {
			log.Warning(fmt.Sprintf("[%s] WARNUNG: Keine Basislinie für Kunde %d Zähler %d vorhanden, ignoriere", modus, kundeNummer, zählerNummer))
		} else {
			ds.ZählersummeSummeBasislinie = zsBasislinieWert
			ds.ZählersummeSummeBasislinieBereinigt = zsBasislinieWert * zsBasislinieFaktor
		}
		datumStr := ds.MessungDatum.Format(isoDatumformat)
		zsWert, zsFaktor, err := holeZählersummeFürDatum(db, kundeNummer, zählerNummer, datumStr)
		if err != nil {
			log.Warning(fmt.Sprintf("[%s] WARNUNG: Keine Basislinie für Kunde %d Zähler %d und Datum %s vorhanden, ignoriere", modus, kundeNummer, zählerNummer, datumStr))
		} else {
			ds.ZählersummeSummeEingespart = zsWert
			ds.ZählersummeSummeEingespartBereinigt = zsWert * zsFaktor
		}

		// Eintrag anlegen
		ergebnis, err = db.Exec(`insert into esz.export (
			konfig_antragsteller_nummer,
			mess_nummer,
			kunde_nummer,
			endku_system_art,
			endku_groesse,
			endku_branche,
			endku_postleitzahl,
			endku_bewohner_anzahl,
			endku_gebaeude_typ,
			endku_baualterklasse,
			endku_sanierung_energ_letzte,
			endku_flaeche_gesamt,
			mess_datum,
			zaehler_id,
			zaehler_id_uebergeordnet,
			zaehler_art,
			zaehler_geraete_art,
			zaehler_geraete_sonstiges_freitext,
			zaehler_typ,
			zaehler_energietraeger,
			mess_zaehlerstand,
			opt_massnahme,
			opt_sonstiges_freitext,
			endku_smg_status,
			zaehlersum_summe_eingespart,
			zaehlersum_summe_eingespart_bereinigt,
			konfig_abfrage_haeufigkeit,
			klimafak_faktor,
			zaehlersum_summe_basislinie,
			zaehlersum_summe_basislinie_bereinigt
			) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)`,
			ds.AntragstellerNummer, ds.MessungNummer, ds.KundeNummer,
			ds.EndkundeSystemArt, ds.EndkundeGröße, ds.EndkundeBranche, &ds.EndkundePostleitzahl, ds.EndkundeBewohnerAnzahl,
			ds.EndkundeGebäudeTyp, ds.EndkundeBaualterklasse, ds.EndkundeSanierungEnergieLetzte, ds.EndkundeFlächeGesamt,
			ds.MessungDatum, ds.ZählerID, ds.ZählerIDÜbergeordnet, ds.ZählerArt, ds.ZählerGeräteArt, ds.ZählerGeräteSonstiges, ds.ZählerTyp,
			ds.ZählerEnergieträger, ds.MessungZählerstand, ds.OptimierungMaßnahme, ds.OptimierungSonstiges, ds.EndkundeSMGStatus,
			ds.ZählersummeSummeEingespart, ds.ZählersummeSummeEingespartBereinigt, ds.Abfragehäufigkeit, ds.Klimafaktor,
			ds.ZählersummeSummeBasislinie, ds.ZählersummeSummeBasislinieBereinigt)
		if err != nil {
			return fmt.Errorf("Kunde %d: Export-Tabelleneintrag für Messung %v anlegen fehlgeschlagen: %s", kundeNummer, ds, err)
		}
		anzZeilen, err = ergebnis.RowsAffected()
		if err != nil {
			return fmt.Errorf("Kunde %d: Export-Tabelleneintrag für Messung %v anlegen fehlgeschlagen (RowsAffected): %s", kundeNummer, ds, err)
		}
		if anzZeilen != 1 {
			return fmt.Errorf("Kunde %d: Export-Tabelleneintrag für Messung %v anlegen:  RowsAffected() ist != 1: %d", kundeNummer, ds, anzZeilen)
		}
		// Erfolg
		log.Debug(fmt.Sprintf("[%s] Kunde %d: Messung %v erfolgreich angelegt", modus, kundeNummer, ds))
	}

	return nil
}

func anzahlZähler(db *sql.DB, kundeNummer int) (anzZähler int, err error) {
	row := db.QueryRow("select count(*) from esz.zaehler where kunde_nummer = $1 and vereinelem_nummer > 0", kundeNummer)
	err = row.Scan(&anzZähler)
	if err != nil {
		return -1, fmt.Errorf("Anzahl Zähler für Kunde %d holen fehlgeschlagen: %s", kundeNummer, err)
	}
	log.Debug(fmt.Sprintf("[%s] Endkunde %d hat %d Zähler mit gültigem Vereinigungselement", modus, kundeNummer, anzZähler))

	return anzZähler, err
}

//TODO oder nach Jahr und Monat
func holeExporttabelle(nachMessungNr int) (datenAus []ExportDatensatz, err error) {
	// Ausgabe initialisieren
	datenAus = []ExportDatensatz{}

	// Datenbankverbindung aufbauen
	// Dokumentation @ https://github.com/golang/go/wiki/SQLInterface
	// NOTE: gorm hat nicht funktioniert, db.Raw().Scan().Error hat immer leeres Ergebnis geliefert
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s sslmode=disable connect_timeout=%d user=%s password=%s dbname=%s", dbAdresse, dbWartezeit, dbBenutzer, dbPasswort, dbDatenbank))
	if err != nil {
		return nil, fmt.Errorf("[%s] Datenbankverbindung fehlgeschlagen: %s", modus, err)
	}
	defer db.Close()
	log.Debug(fmt.Sprintf("[%s] Datenbankverbindung erfolgreich", modus))

	einträge, err := db.Query(`
		select
			konfig_antragsteller_nummer,
			mess_nummer,
			kunde_nummer,
			endku_system_art,
			endku_groesse,
			endku_branche,
			endku_postleitzahl,
			endku_bewohner_anzahl,
			endku_gebaeude_typ,
			endku_baualterklasse,
			endku_sanierung_energ_letzte,
			endku_flaeche_gesamt,
			mess_datum,
			zaehler_id,
			zaehler_id_uebergeordnet,
			zaehler_art,
			zaehler_geraete_art,
			zaehler_geraete_sonstiges_freitext,
			zaehler_typ,
			zaehler_energietraeger,
			mess_zaehlerstand,
			opt_massnahme,
			opt_sonstiges_freitext,
			endku_smg_status,
			zaehlersum_summe_eingespart,
			zaehlersum_summe_eingespart_bereinigt,
			konfig_abfrage_haeufigkeit,
			klimafak_faktor,
			-- export_einfluss* und export_nutzen* entfällt (leer im Export)
			zaehlersum_summe_basislinie,
			zaehlersum_summe_basislinie_bereinigt
			-- export_einfluss*_basislinie_gemittelt und export_nutzen*_basislinie_gemittelt entfällt (leer im Export)
		from esz.export
		where mess_nummer > $1`,
		nachMessungNr)
	if err != nil {
		return nil, fmt.Errorf("Export-Tabelle abfragen: %s", err)
	}
	//var szNummer, zuständigesElementNummer int
	//var gültigVon time.Time
	//var gültigBis pq.NullTime
	var letzteMessung int
	for einträge.Next() {
		ds := ExportDatensatz{}
		if err = einträge.Scan(
			&ds.AntragstellerNummer, &ds.MessungNummer, &ds.KundeNummer,
			&ds.EndkundeSystemArt, &ds.EndkundeGröße, &ds.EndkundeBranche, &ds.EndkundePostleitzahl, &ds.EndkundeBewohnerAnzahl,
			&ds.EndkundeGebäudeTyp, &ds.EndkundeBaualterklasse, &ds.EndkundeSanierungEnergieLetzte, &ds.EndkundeFlächeGesamt,
			&ds.MessungDatum, &ds.ZählerID, &ds.ZählerIDÜbergeordnet, &ds.ZählerArt, &ds.ZählerGeräteArt, &ds.ZählerGeräteSonstiges, &ds.ZählerTyp,
			&ds.ZählerEnergieträger, &ds.MessungZählerstand, &ds.OptimierungMaßnahme, &ds.OptimierungSonstiges, &ds.EndkundeSMGStatus,
			&ds.ZählersummeSummeEingespart, &ds.ZählersummeSummeEingespartBereinigt, &ds.Abfragehäufigkeit, &ds.Klimafaktor,
			&ds.ZählersummeSummeBasislinie, &ds.ZählersummeSummeBasislinieBereinigt); err != nil {
			return nil, fmt.Errorf("Messung (letzte erfolgreiche: %d) in Datenstruktur einlesen: %s", letzteMessung, err)
		}
		letzteMessung = ds.MessungNummer
		//TODO if debug -> Eintrag ausgeben
		//TODO Zeitangaben die leer sein können? NullTimeToTime(gültigBis),
		datenAus = append(datenAus, ds)
	}
	if err = einträge.Err(); err != nil {
		return nil, fmt.Errorf("Einträge aus Export-Tabelle auslesen: %s", err)
	}

	// Erfolg
	log.Info(fmt.Sprintf("[%s] Einträge in Export-Tabelle gefunden: %d", modus, len(datenAus)))
	einträge.Close()

	return datenAus, err
}
