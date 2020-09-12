package main

import (
	"context"
	"github.com/jackc/pgx/v4"
	"log"
)

const defaultDb = "dbname=steve"

func main() {
	setup()
	ctx := context.Background()

	log.Print("=== With statement cache ===")
	cfg, err := pgx.ParseConfig(defaultDb)
	if err != nil {
		log.Fatalf("failed to parse configuration: %v", err)
	}
	db, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	test(db)
	_ = db.Close(ctx)

	log.Print("=== Without statement cache ===")
	cfg, err = pgx.ParseConfig(defaultDb)
	if err != nil {
		log.Fatalf("failed to parse configuration: %v", err)
	}
	cfg.BuildStatementCache = nil
	db, err = pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	test(db)
	_ = db.Close(ctx)
}
func test(db *pgx.Conn) {
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("failed to get tx: %v", err)
	}

	var rows pgx.Rows
	log.Print("first select")
	rows, err = tx.Query(ctx, `select bar, idx from foo where bar = $2 and idx >= $1 order by idx`, 0, "baz")

	if err != nil {
		log.Fatalf("select 1: %v", err)
	}

	for rows.Next() {
		var bar string
		var idx int
		err = rows.Scan(&bar, &idx)
		log.Printf("  read (%s, %d) err=%v", bar, idx, err)
	}

	err = tx.Rollback(ctx)
	if err != nil {
		log.Fatalf("rollback: %v\n", err)
	}

	tx, err = db.Begin(context.Background())
	if err != nil {
		log.Fatalf("failed to get tx: %v", err)
	}

	log.Print("dropping table")
	_, err = tx.Exec(ctx, "drop table foo")
	if err != nil {
		log.Fatalf("drop table: %v\n", err)
	}

	log.Print("second select")
	rows, err = tx.Query(ctx, `select bar, idx from foo where bar = $2 and idx >= $1 order by idx`, 0, "baz")

	if err != nil {
		log.Printf("select from foo err=%v\n", err)
		return
	} else {
		log.Printf("Expected error, but no error selecting from foo")
	}

	for rows.Next() {
		var bar string
		var idx int
		err = rows.Scan(&bar, &idx)
		if err != nil {
			log.Fatalf("scanning result: %v", err)
		}
		log.Printf("  read (%s, %d) err=%v", bar, idx, err)
	}
	err = tx.Rollback(ctx)
	if err != nil {
		log.Fatalf("rollback: %v\n", err)
	}
}

func setup() {
	ctx := context.Background()
	testDb, err := pgx.Connect(ctx, defaultDb)
	if err != nil {
		log.Fatalf("setup: failed to connect to database: %v", err)
	}
	_, err = testDb.Exec(ctx, "drop table if exists foo")
	if err != nil {
		log.Fatalf("setup: dropping failed: %v", err)
	}
	_, err = testDb.Exec(ctx, `create table foo (bar text, idx int)`)
	if err != nil {
		log.Fatalf("setup: creating failed: %v", err)
	}
	_, err = testDb.Exec(ctx, `insert into foo (bar, idx) values ('baz', 42)`)
	if err != nil {
		log.Fatalf("setup: population failed: %v", err)
	}
}
