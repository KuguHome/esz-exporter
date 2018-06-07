package main

import (
	"database/sql"
	"fmt"
	"math"
	"time"
)

// Jährlicher Eintrag
func zählerSummeErstellen(zeitpunkt time.Time, erledigt chan<- error) {
	// Datenbankverbindung herstellen
	var db *sql.DB
	var err error
	if db, err = verbindeDatenbank(); err != nil {
		erledigt <- err
		return
	}

	log.Info(fmt.Sprintf("[%s] Erstelle nächste Zählersumme für Kunde %d Zähler %d", modus, kundeNummer, zählerNummer))

	// letzte Zählersumme holen
	vonDatumLetztes, bisDatumLetztes, _, err := holeLetzteZählersumme(db, kundeNummer, zählerNummer)
	if err != nil {
		erledigt <- err
		return
	}

	// neues vonDatum und bisDatum berechnen
	bisDatumLetztesTime, err := time.Parse(isoDatumformat, bisDatumLetztes)
	if err != nil {
		erledigt <- err
		return
	}
	vonDatumNeu := bisDatumLetztesTime.AddDate(0, 0, 1) // Beginn am nächsten Tag
	bisDatumNeu := bisDatumLetztesTime.AddDate(1, 0, 0) // Dauer 1 Jahr
	vonDatumNeuStr := vonDatumNeu.Format(isoDatumformat)
	bisDatumNeuStr := bisDatumNeu.Format(isoDatumformat)
	log.Debug(fmt.Sprintf("[%s] Zählersumme alte Periode war von %s bis %s, daher ist nächste Periode von %s bis %s", modus, vonDatumLetztes, bisDatumLetztes, vonDatumNeuStr, bisDatumNeuStr))

	// Kontrolle auf Vergangenheit
	if bisDatumNeu.After(time.Now()) {
		erledigt <- fmt.Errorf("[%s] FEHLER: Periode für nächste zu erstellende Zählersumme liegt noch in der Zukunft: von %s bis %s", modus, vonDatumNeuStr, bisDatumNeuStr)
		return
	}

	// Messung zu Beginn und zu Ende der neuen Periode finden
	zählerStandAnf, err := holeMessung(db, kundeNummer, zählerNummer, vonDatumNeuStr)
	if err != nil {
		erledigt <- err //TODO schönere Fehlermeldung
		return
	}
	zählerStandEnde, err := holeMessung(db, kundeNummer, zählerNummer, bisDatumNeuStr)
	if err != nil {
		erledigt <- err //TODO schönere Fehlermeldung
		return
	}

	// Differenz berechnen = unbereinigter Verbrauch
	verbrauch := zählerStandEnde - zählerStandAnf

	// aktuell gültigen Klimafaktor holen (Kontrolle ob vorhanden)
	plz, err := holeKundePLZ(db, kundeNummer)
	if err != nil {
		erledigt <- err //TODO schönere Fehlermeldung
		return
	}
	klimaFaktor, err := holeKlimafaktor(db, bisDatumNeuStr, plz)
	if err != nil {
		erledigt <- err //TODO schönere Fehlermeldung
		return
	}

	// Eintrag in esz.zaehlersumme anlegen
	err = speichereZählersumme(db, kundeNummer, zählerNummer, vonDatumNeuStr, bisDatumNeuStr, verbrauch, klimaFaktor)
	if err != nil {
		erledigt <- err
		return
	}

	// Erfolg
	log.Info(fmt.Sprintf("[%s] Neue Zählersumme für Kunde %d Zähler %d für Periode von %s bis %s erfolgreich angelegt mit Summe %.5f und Klimafaktor %.2f", modus, kundeNummer, zählerNummer, vonDatumNeuStr, bisDatumNeuStr, verbrauch, klimaFaktor))

	// Datenbankverbindung trennen
	trenneDatenbank(db)

	erledigt <- nil
	return
}

func holeKundePLZ(db *sql.DB, kundeNummer int) (plz int, err error) {
	row := db.QueryRow("select endku_postleitzahl from esz.endkunde where kunde_nummer = $1;", kundeNummer)
	err = row.Scan(&plz)
	if err != nil {
		return 0, fmt.Errorf("Postleitzahl für Kunde %d holen fohlgeschlagen: %s", kundeNummer, err)
	}
	log.Debug(fmt.Sprintf("[%s] Kunde %d hat PLZ %d", modus, kundeNummer, plz))

	return plz, nil
}

func holeKlimafaktor(db *sql.DB, datumStr string, plz int) (faktor float64, err error) {
	//datumStr := zeitpunkt.Format(isoDatumformat)
	row := db.QueryRow("select klimafak_faktor from esz.klimafaktor where klimafak_postleitzahl = $1 and $2 between klimafak_von_datum and klimafak_bis_datum;", plz, datumStr)
	err = row.Scan(&faktor)
	if err != nil {
		return math.NaN(), fmt.Errorf("Klimafaktor für PLZ %d zum Zeitpunkt %s holen fehlgeschlagen: %s", plz, datumStr, err)
	}
	log.Debug(fmt.Sprintf("[%s] PLZ %d hat zum Zeitpunkt %s Klimafaktor %.3f", modus, plz, datumStr, faktor))

	return faktor, nil
}

func holeLetzteZählersumme(db *sql.DB, kundeNummer int, zählerNummer int) (vonDatum string, bisDatum string, summe float64, err error) {
	row := db.QueryRow("select zaehlersum_von_datum, zaehlersum_bis_datum, zaehlersum_summe from esz.zaehlersumme where kunde_nummer = $1 and zaehler_nummer = $2 order by zaehlersum_bis_datum desc limit 1;", kundeNummer, zählerNummer)
	var vonDatumTime, bisDatumTime time.Time //TODO testen
	err = row.Scan(&vonDatumTime, &bisDatumTime, &summe)
	if err != nil {
		return "", "", math.NaN(), fmt.Errorf("Letzte Zählersumme für Kunde %d und Zähler %d holen fehlgeschlagen: %s", kundeNummer, zählerNummer, err)
	}
	vonDatum = vonDatumTime.Format(isoDatumformat)
	bisDatum = bisDatumTime.Format(isoDatumformat)
	log.Debug(fmt.Sprintf("[%s] Endkunde %d Zähler %d hat letzte Zählersumme von %s bis %s", modus, kundeNummer, zählerNummer, vonDatum, bisDatum))

	return vonDatum, bisDatum, summe, nil
}

func holeZählersummeFürDatum(db *sql.DB, kundeNummer int, zählerNummer int, datum string) (summe float64, klimaFaktor float64, err error) {
	row := db.QueryRow("select zaehlersum_summe, zaehlersum_bereinigungsfaktor from esz.zaehlersumme where kunde_nummer = $1 and zaehler_nummer = $2 and $3 between zaehlersum_von_datum and zaehlersum_bis_datum limit 1;", kundeNummer, zählerNummer, datum)
	err = row.Scan(&summe, &klimaFaktor)
	if err != nil {
		return math.NaN(), math.NaN(), fmt.Errorf("Holen der Zählersumme für Kunde %d Zähler %d und Datum %s holen fehlgeschlagen: %s", kundeNummer, zählerNummer, datum, err)
	}
	log.Debug(fmt.Sprintf("[%s] Endkunde %d Zähler %d hat am %s die zuständige Zählersumme %.5f mit Klimafaktor %.2f", modus, kundeNummer, zählerNummer, datum, summe, klimaFaktor))

	return summe, klimaFaktor, nil
}

func holeZählersummeBasislinie(db *sql.DB, kundeNummer int, zählerNummer int) (vonDatum string, bisDatum string, summe float64, klimaFaktor float64, err error) {
	row := db.QueryRow("select zaehlersum_von_datum, zaehlersum_bis_datum from esz.zaehlersumme where zaehlersum_zeitraum_art = 'Basislinie' and kunde_nummer = $1 and zaehler_nummer = $2 order by zaehlersum_von_datum asc limit 1;", kundeNummer, zählerNummer)
	var vonDatumTime, bisDatumTime time.Time //TODO testen
	err = row.Scan(&vonDatumTime, &bisDatumTime, &summe, &klimaFaktor)
	if err != nil {
		return "", "", math.NaN(), math.NaN(), fmt.Errorf("Letzte Zählersumme für Kunde %d und Zähler %d holen fehlgeschlagen: %s", kundeNummer, zählerNummer, err)
	}
	vonDatum = vonDatumTime.Format(isoDatumformat)
	bisDatum = bisDatumTime.Format(isoDatumformat)
	log.Debug(fmt.Sprintf("[%s] Endkunde %d Zähler %d hat Basislinie von %s bis %s mit Summe %.5f Klimafaktor %.2f", modus, kundeNummer, zählerNummer, vonDatum, bisDatum, summe, klimaFaktor))

	return vonDatum, bisDatum, summe, klimaFaktor, nil
}

func holeMessung(db *sql.DB, kundeNummer int, zählerNummer int, datum string) (zählerStand float64, err error) {
	row := db.QueryRow("select mess_zaehlerstand from esz.messung where kunde_nummer = $1 and zaehler_nummer = $2 and mess_datum = $3;", kundeNummer, zählerNummer, datum)
	err = row.Scan(&zählerStand)
	if err != nil {
		return math.NaN(), fmt.Errorf("Zählerstand für Kunde %d Zähler %d Datum %s holen fehlgeschlagen: %s", kundeNummer, zählerNummer, datum, err)
	}
	log.Debug(fmt.Sprintf("[%s] Endkunde %d Zähler %d hat mit Datum %s den Zählerstand %.5f", modus, kundeNummer, zählerNummer, datum, zählerStand))

	return zählerStand, nil
}

// erstellt eine jährliche Zählersumme
func speichereZählersumme(db *sql.DB, kundeNummer int, zählerNummer int, vonDatum string, bisDatum string, summe float64, klimaFaktor float64) (err error) {
	// Eintrag anlegen
	ergebnis, err := db.Exec(`insert into esz.zaehlersumme (
    kunde_nummer,
    zaehler_nummer,
    zaehlersum_zeitraum_art,
    zaehlersum_von_datum,
    zaehlersum_bis_datum,
    -- unbereinigt
    zaehlersum_summe,
    zaehlersum_bereinigungsfaktor
    ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)`,
		kundeNummer, zählerNummer, "jährlich", vonDatum, bisDatum, summe, klimaFaktor)
	if err != nil {
		return fmt.Errorf("Kunde %d: Zählersumme-Eintrag anlegen für Zähler %d, von %s bis %s, Summe %.5f, Faktor %.2f fehlgeschlagen: %s", kundeNummer, zählerNummer, vonDatum, bisDatum, summe, klimaFaktor, err)
	}
	anzZeilen, err := ergebnis.RowsAffected()
	if err != nil {
		return fmt.Errorf("Kunde %d: Zählersumme-Eintrag anlegen für Zähler %d, von %s bis %s, Summe %.5f, Faktor %.2f fehlgeschlagen (RowsAffected): %s", kundeNummer, zählerNummer, vonDatum, bisDatum, summe, klimaFaktor, err)
	}
	if anzZeilen != 1 {
		return fmt.Errorf("Kunde %d: Zählersumme-Eintrag anlegen für Zähler %d, von %s bis %s, Summe %.5f, Faktor %.2f fehlgeschlagen: RowsAffected() ist != 1: %d", kundeNummer, zählerNummer, vonDatum, bisDatum, summe, klimaFaktor, anzZeilen)
	}

	// Erfolg
	log.Debug(fmt.Sprintf("[%s] Kunde %d Zähler %d: Zählersumme für %s bis %s mit Summe %.4f und Faktor %.2f erfolgreich angelegt", modus, kundeNummer, zählerNummer, vonDatum, bisDatum, summe, klimaFaktor))
	return nil
}
