CREATE TABLE "prosports.data.transactions" (
	"id"	INTEGER,
	"date"	INTEGER,
	"team"	INTEGER,
	"acquired"	INTEGER,
	"relinquished"	INTEGER,
	"notes"	INTEGER,
	"league"	INTEGER,
	"transaction_type"	INTEGER,
	PRIMARY KEY("id" AUTOINCREMENT),
	CONSTRAINT "uq_pro_sports_transactions" UNIQUE("date","team","acquired","relinquished","notes","league","transaction_type")
)