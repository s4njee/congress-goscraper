package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

var Tables = [8]string{"s", "hr", "hconres", "hjres", "hres", "sconres", "sjres", "sres"}
var mutex = &sync.Mutex{}

type XMLSummaries struct {
	XMLName          xml.Name         `xml:"summaries"`
	XMLBillSummaries XMLBillSummaries `xml:"billSummaries"`
}

type XMLBillSummaries struct {
	XMLName      xml.Name               `xml:"billSummaries"`
	XMLBillItems []XMLBillSummariesItem `xml:"item"`
}
type XMLBillSummariesItem struct {
	XMLName xml.Name `xml:"item"`
	Date    string   `xml:"lastSummaryUpdateDate"`
	Text    string   `xml:"text"`
}
type BillXMLRoot struct {
	XMLName xml.Name `xml:"billStatus"`
	BillXML BillXML  `xml:"bill"`
}
type ItemXML struct {
	XMLName xml.Name `xml:"item"`
	ActedAt string   `xml:"actionDate"`
	Text    string   `xml:"text"`
	Type    string   `xml:"type"`
}
type ActionsXML struct {
	XMLName xml.Name  `xml:"actions"`
	Actions []ItemXML `xml:"item"`
}
type SponsorXML struct {
	XMLName xml.Name `xml:"item"`
	Name    string   `xml:"fullName"`
	State   string   `xml:"state"`
}
type SponsorsXML struct {
	XMLName  xml.Name     `xml:"sponsors"`
	Sponsors []SponsorXML `xml:"item"`
}
type CosponsorXML struct {
	XMLName xml.Name `xml:"item"`
	Name    string   `xml:"fullName"`
	State   string   `xml:"state"`
}
type CosponsorsXML struct {
	XMLName    xml.Name     `xml:"cosponsors"`
	Cosponsors []SponsorXML `xml:"item"`
}
type BillXML struct {
	XMLName      xml.Name      `xml:"bill"`
	Number       string        `xml:"billNumber"`
	BillType     string        `xml:"billType"`
	IntroducedAt string        `xml:"introducedDate"`
	Congress     string        `xml:"congress"`
	Summary      XMLSummaries  `xml:"summaries"`
	Actions      ActionsXML    `xml:"actions"`
	Sponsors     SponsorsXML   `xml:"sponsors"`
	Cosponsors   CosponsorsXML `xml:"cosponsors"`
	ShortTitle   string        `xml:"title"`
}
type Bill struct {
	bun.BaseModel `bun:"table:bills"`
	Number        string `bun:",pk"`
	BillType      string `json:"bill_type" bun:",pk"`
	IntroducedAt  string `json:"introduced_at"`
	Congress      string `bun:",pk"`
	Summary       struct {
		Date string
		Text string
	} `json:"summary,omitempty"`
	Actions []struct {
		ActedAt string
		Text    string
		Type    string
	} `json:"actions,omitempty"`
	Sponsors []struct {
		Title    string `json:"omitempty"`
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"sponsors,omitempty"`
	Cosponsors []struct {
		Title    string `json:"omitempty"`
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"cosponsors,omitempty"`
	StatusAt      string `json:"status_at""`
	ShortTitle    string `json:"short_title"`
	OfficialTitle string `json:"official_title"`
}

type BillJSON struct {
	bun.BaseModel `bun:"table:bills"`
	Number        string
	BillType      string `json:"bill_type"`
	IntroducedAt  string `json:"introduced_at"`
	Congress      string
	Summary       struct {
		Date string
		Text string
	} `json:"summary,omitempty"`
	Actions []struct {
		ActedAt string `json:"acted_at"`
		Text    string
		Type    string
	} `json:"actions,omitempty"`
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"sponsors,omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"district,omitempty"`
	} `json:"cosponsors,omitempty"`

	StatusAt      string `json:"status_at""`
	ShortTitle    string `json:"short_title"`
	OfficialTitle string `json:"official_title"`
}

func parse_bill(path string) *BillJSON {
	jsonFile, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var billjs BillJSON

	json.Unmarshal(byteValue, &billjs)
	return &billjs

}

func parse_bill_xml(path string) *Bill {
	xmlFile, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
	}

	byteValue, _ := ioutil.ReadAll(xmlFile)
	var billxml BillXMLRoot
	xml.Unmarshal(byteValue, &billxml)

	var action_structs []struct {
		ActedAt string
		Text    string
		Type    string
	}

	// Create structs with same hiearchy as billJSON, so that data is uniform when inserted into postgreSQL
	for _, action := range billxml.BillXML.Actions.Actions {
		action_structs = append(action_structs, struct {
			ActedAt string
			Text    string
			Type    string
		}{
			ActedAt: action.ActedAt,
			Text:    action.Text,
			Type:    action.Type,
		})
	}
	var sponsor_structs []struct {
		Title    string `json:"omitempty"`
		Name     string
		State    string
		District string `json:"omitempty"`
	}

	for _, sponsor := range billxml.BillXML.Sponsors.Sponsors {
		sponsor_structs = append(sponsor_structs, struct {
			Title    string `json:"omitempty"`
			Name     string
			State    string
			District string `json:"omitempty"`
		}{
			Name:  sponsor.Name,
			State: sponsor.State,
		})
	}
	var cosponsor_structs []struct {
		Title    string `json:"omitempty"`
		Name     string
		State    string
		District string `json:"omitempty"`
	}

	for _, cosponsor := range billxml.BillXML.Cosponsors.Cosponsors {
		cosponsor_structs = append(cosponsor_structs, struct {
			Title    string `json:"omitempty"`
			Name     string
			State    string
			District string `json:"omitempty"`
		}{
			Name:  cosponsor.Name,
			State: cosponsor.State,
		})
	}
	var Date string
	var Text string
	if billxml.BillXML.Summary.XMLBillSummaries.XMLBillItems != nil {
		Date = billxml.BillXML.Summary.XMLBillSummaries.XMLBillItems[0].Date
		Text = billxml.BillXML.Summary.XMLBillSummaries.XMLBillItems[0].Text
	}
	summary := struct {
		Date string
		Text string
	}{
		Date: Date,
		Text: Text,
	}

	// Create Bill Struct, same fields as BillJSON
	var bill = Bill{
		Number:        billxml.BillXML.Number,
		BillType:      strings.ToLower(billxml.BillXML.BillType),
		IntroducedAt:  billxml.BillXML.IntroducedAt,
		Congress:      billxml.BillXML.Congress,
		Summary:       summary,
		Actions:       action_structs,
		Sponsors:      sponsor_structs,
		Cosponsors:    cosponsor_structs,
		StatusAt:      billxml.BillXML.Actions.Actions[0].ActedAt,
		ShortTitle:    billxml.BillXML.ShortTitle,
		OfficialTitle: billxml.BillXML.ShortTitle,
	}
	return &bill
}
func main() {

	ctx := context.Background()
	dsn := "postgres://postgres:postgres@localhost:5432/csearch?sslmode=disable"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	db := bun.NewDB(sqldb, pgdialect.New())

	// Create db code
	var expr = fmt.Sprintf("DROP TABLE IF EXISTS bills CASCADE;")
	println(expr)
	db.Exec(expr)
	_, err := db.NewCreateTable().
		Model((*Bill)(nil)).
		PartitionBy("LIST (bill_type)").
		Exec(ctx)
	if err != nil {
		panic(err)
	}

	// Create db partitions
	for _, i := range Tables {
		var expr = fmt.Sprintf("CREATE TABLE bills_t%s PARTITION OF bills FOR VALUES in ('%s');", i, i)
		var expr2 = fmt.Sprintf("CREATE INDEX ON bills_t%s ('bill_type');", i)
		println(expr)
		db.Exec(expr)
		println(expr2)
		db.Exec(expr2)
		var expr3 = fmt.Sprintf("ALTER TABLE bills ADD COLUMN %s_ts tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(short_title,'') || ' ' || coalesce(summary,''))) STORED;", i)
		var expr4 = fmt.Sprintf("CREATE INDEX bill_ts_idx ON bills USING GIN (%s_ts);", i)
		println(expr3)
		db.Exec(expr3)
		println(expr4)
		db.Exec(expr4)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for i := 93; i <= 117; i++ {
		for _, table := range Tables {
			files, err := ioutil.ReadDir(fmt.Sprintf("../congress_api/scraper/congress/data/%s/bills/%s", strconv.Itoa(i), table))
			if err != nil {
				debug.PrintStack()
				continue
			}
			var bills []*Bill
			var billsJSON []*BillJSON
			wg.Add(len(files))
			println(len(files))
			for _, f := range files {
				path := fmt.Sprintf("../congress_api/scraper/congress/data/%s/bills/%s/", strconv.Itoa(i), table) + f.Name()
				var xmlcheck = path + "/fdsys_billstatus.xml"
				if _, err := os.Stat(xmlcheck); err == nil {
					go func() {
						defer mutex.Unlock()
						sem <- struct{}{}
						mutex.Lock()
						bills = append(bills, parse_bill_xml(xmlcheck))
						defer func() { <-sem }()
						defer wg.Done()
					}()

				} else if errors.Is(err, os.ErrNotExist) {
					path += "/data.json"
					go func() {
						defer mutex.Unlock()
						sem <- struct{}{}
						var bjs = parse_bill(path)
						// print(bjs.ShortTitle)
						mutex.Lock()
						billsJSON = append(billsJSON, bjs)
						defer func() { <-sem }()
						defer wg.Done()
					}()
				}

			}
			wg.Wait()

			if len(bills) > 0 {
				res, err := db.NewInsert().Model(&bills).Exec(ctx)
				fmt.Printf("Congress: %s Type: %s Inserted %s rows", strconv.Itoa(i), table, strconv.Itoa(len(bills)))
				if err != nil {
					panic(err)
				} else {
					fmt.Println(res)
				}
			} else if len(billsJSON) > 0 {
				res, err := db.NewInsert().Model(&billsJSON).Exec(ctx)
				fmt.Printf("Congress: %s Type: %s Inserted %s rows", strconv.Itoa(i), table, strconv.Itoa(len(billsJSON)))
				if err != nil {
					panic(err)
				} else {
					fmt.Println(res)
				}
			}
		}
	}
	close(sem)
}
