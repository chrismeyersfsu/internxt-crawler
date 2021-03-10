package main

import (
	"database/sql"
	"time"
)

func openDB(fname string) *sql.DB {
	database, _ := sql.Open("sqlite3", fname)
	statement, _ := database.Prepare(`CREATE TABLE IF NOT EXISTS contacts (
		id INTEGER PRIMARY KEY,
		gathered_ts DATE DEFAULT (datetime('now','localtime')),
		NodeID TEXT, 
		SpaceAvailable INTEGER, 
		LastTimeout INTEGER,
		TimeoutRate REAL,
		ResponseTime REAL,
		Reputation INTEGER,
		LastSeen INTEGER,
		Address TEXT,
		IP TEXT,
		Protocol TEXT,
		UserAgent Text);`)
	statement.Exec()
	return database
}

func writeContact(database *sql.DB, c Contact, epoch time.Time) {
	statement, err := database.Prepare(`INSERT into contacts (
			gathered_ts,
			NodeID,
			SpaceAvailable,
			LastTimeout,
			TimeoutRate,
			ResponseTime,
			Reputation,
			LastSeen,
			Address,
			IP,
			Protocol,
			UserAgent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	checkErr(err)
	statement.Exec(
		epoch,
		c.NodeID,
		bool2IntStr(c.SpaceAvailable),
		c.LastTimeout,
		c.TimeoutRate,
		c.ResponseTime,
		c.Reputation,
		c.LastSeen,
		c.Address,
		c.IP,
		c.Protocol,
		c.UserAgent,
	)
}

func writeContacts(database *sql.DB, contacts []Contact, epoch time.Time) {
	for i := 0; i < len(contacts); i++ {
		var c Contact = contacts[i]
		writeContact(database, c, epoch)
	}
}

func bool2IntStr(val bool) string {
	if val {
		return "1"
	}
	return "0"
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
