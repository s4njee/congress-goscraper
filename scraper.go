package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
)

var Tables = [8]string{"s", "hr", "hconres", "hjres", "hres", "sconres", "sjres", "sres"}
var mutex = &sync.Mutex{}

type Bill struct {
	Number       string
	BillType     string `json:"bill_type"`
	IntroducedAt string `json:"introduced_at"`
	Congress     string
	Summary      Summary `json:"summary,omitempty"`
	Actions      []struct {
		ActedAt string `json:"acted_at"`
		Text    string
		Type    string
	} `json:"actions"`
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"sponsors"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"district,omitempty"`
	} `json:"cosponsors"`
	StatusAt      string `json:"status_at""`
	ShortTitle    string `json:"short_title"`
	OfficialTitle string `json:"official_title"`
}
type Summary struct {
	As   string
	Date string
	Text string
}

func parse_bill(path string) Bill {
	defer mutex.Unlock()
	jsonFile, err := os.Open(path)
	// if wei os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	// read our opened xmlFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)

	var bill Bill
	json.Unmarshal(byteValue, &bill)
	mutex.Lock()
	return bill
}

func main() {
	ctx := context.Background()
	dsn := "postgres://postgres:postgres@localhost:5432/csearch?sslmode=disable"
	// dsn := "unix://user:pass@dbname/var/run/postgresql/.s.PGSQL.5431"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	db := bun.NewDB(sqldb, pgdialect.New())
	//_, err := db.NewCreateTable().
	//	Model((*Bill)(nil)).
	//	PartitionBy("LIST (bill_type)").
	//	Exec(ctx)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//for _, i := range Tables {
	//	var expr = fmt.Sprintf("CREATE TABLE bills_t%s PARTITION OF bills FOR VALUES in ('%s');", i, i)
	//	print(expr)
	//	db.Exec(expr)
	//}
	var wg sync.WaitGroup
	for i := 93; i <= 117; i++ {
		for _, table := range Tables {
			files, err := ioutil.ReadDir(fmt.Sprintf("../../congress/data/%s/bills/%s", strconv.Itoa(i), table))
			if err != nil {
				log.Fatal(err)
				continue
			}
			var bills []Bill
			wg.Add(len(files))
			for _, f := range files {
				path := fmt.Sprintf("../../congress/data/%s/bills/%s/", strconv.Itoa(i), table) + f.Name()
				path += "/data.json"
				go func() {
					bills = append(bills, parse_bill(path))
					wg.Done()
				}()
			}
			wg.Wait()
			db.NewInsert().Model(&bills).Exec(ctx)

			//for _, bill := range bills {
			//	db.NewInsert().Model(bill).Exec(ctx)
			//	//fmt.Printf("Processed %s %s-%s", bill.Congress, bill.BillType, bill.Number)
			//}
		}
	}

}
