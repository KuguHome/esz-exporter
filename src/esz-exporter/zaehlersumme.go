package main

import "time"

// Jährliche Einträge in Tabelle Zählersumme beinhalten die Zählerstände und eingesparte Energie

func zählerSummeErstellen(jetzt time.Time, erledigt chan<- error) {
	//TODO implementieren

	// Messung mit angegebenem Datum finden  //TODO oder Datum der letzten Zählersumme hernehmen?

	// Messung für 1 Jahr davor finden

	// Differenz berechnen

	// unbereinigten Wert berechnen

	// aktuell gültigen Klimafaktor holen -- erkennen ob der schon aktualisiert wurde für aktuelles Jahr

	// bereinigten Wert berechnen

	// Eintrag in esz.zaehlersumme anlegen

	// Erfolg
}
