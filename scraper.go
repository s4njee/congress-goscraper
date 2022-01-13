package main

import (
	// "context"
	// "database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	// "github.com/uptrace/bun"
	// "github.com/uptrace/bun/dialect/pgdialect"
	// "github.com/uptrace/bun/driver/pgdriver"
	"io/ioutil"
	// "log"
	"os"
	// "strconv"
	"sync"
)

var Tables = [8]string{"s", "hr", "hconres", "hjres", "hres", "sconres", "sjres", "sres"}
var mutex = &sync.Mutex{}
type XMLSummaries struct{
	XMLName xml.Name `xml:"summaries"`
	XMLBillSummaries XMLBillSummaries `xml:"billSummaries"`
}

type XMLBillSummaries struct{
	XMLName xml.Name `xml:"billSummaries"`
	XMLBillItems []XMLBillSummariesItem `xml:"item"`
}
type XMLBillSummariesItem struct{
	XMLName xml.Name `xml:"item"`
	LastSummaryUpdateDate string `xml:"lastSummaryUpdateDate"`
	Text string `xml:"text"`
}
type BillXMLRoot struct {
    XMLName xml.Name `xml:"billStatus"`
    BillXML   BillXML   `xml:"bill"`
}
type BillXML struct {
	XMLName xml.Name `xml:"bill"`
	Number       string `xml:"billNumber"`  
	BillType     string `xml:"billType"`
	IntroducedAt string `xml:"introducedDate"`
	Congress     string `xml:"congress"`
	Summary      XMLSummaries `xml:"summaries"` 
	Actions      []struct {
		ActedAt string `xml:"actionDate"`
		Text    string `xml:"text"`
		Type    string `xml:"type"`
	} `xml:"actions"`
	Sponsors []struct {
		Name     string `xml:"fullName"`
		State    string `xml:"state"`
	} `xml:"sponsors"`
	Cosponsors []struct {
		Name     string `xml:"fullName"`
		State    string `xml:"state"`
	} `xml:"cosponsors"`
	ShortTitle    string `xml:"title"`
}
type Bill struct {
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type S struct {
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type HR struct {
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type HRES struct {
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type HCONRES struct {
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type HJRES struct {
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type SRES struct {
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type SJRES{
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type SCONRES{
	Number       string
	BillType     string 
	IntroducedAt string
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"omitempty"`
	Actions      []struct {
		ActedAt string 
		Text    string
		Type    string
	} `json:"omitempty"` 
	Sponsors []struct {
		Title    string
		Name     string
		State    string
		District string
	} `json:"omitempty"`
	Cosponsors []struct {
		Title    string
		Name     string
		State    string
		District string `json:"omitempty"`
	} `json:"omitempty"`
	StatusAt      string 
	ShortTitle    string 
	OfficialTitle string 
}
type BillJSON struct {
	Number       string
	BillType     string `json:"bill_type"`
	IntroducedAt string `json:"introduced_at"`
	Congress     string
	Summary      struct {
		Date string
		Text    string
	} `json:"actions,omitempty"`
	Actions      []struct {
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

	var billjs BillJSON
	json.Unmarshal(byteValue, &bill)
	record,_:=json.Marshal(billjs)
	switch {
	case ToUpper(billjs.BillType) == "S":
		var bill S
		json.Unmarshal([]byte(record), &bill)
	case ToUpper(billjs.BillType) == "HR":
		var bill HR
		json.Unmarshal([]byte(record), &bill)
	case ToUpper(billjs.BillType) == "HCONRES":
		var bill HCONRES
		json.Unmarshal([]byte(record), &bill)
	case ToUpper(billjs.BillType) == "HJRES":
		var bill HCONRES
		json.Unmarshal([]byte(record), &bill)
	case ToUpper(billjs.BillType) == "HRES":
		var bill HCONRES
		json.Unmarshal([]byte(record), &bill)
	case ToUpper(billjs.BillType) == "SRES":
		var bill HCONRES
		json.Unmarshal([]byte(record), &bill)
	case ToUpper(billjs.BillType) == "SJRES":
		var bill HCONRES
		json.Unmarshal([]byte(record), &bill)
	case ToUpper(billjs.BillType) == "SCONRES":
		var bill HCONRES
		json.Unmarshal([]byte(record), &bill)
	}
	mutex.Lock()
	return bill
}

func main() {

    xmlFile, err := os.Open("fdsys_billstatus.xml")
    // if we os.Open returns an error then handle it
    if err != nil {
        fmt.Println(err)
    }

	byteValue, _ := ioutil.ReadAll(xmlFile)

	// we initialize our Users array
	var billxml BillXMLRoot
	// we unmarshal our byteArray which contains our
	// xmlFiles content into 'users' which we defined above
	// xml.Unmarshal(byteValue, &billxml)
	// var bill Bill
	// bill.Number = billxml.BillXML.Number
	// bill.BillType = billxml.BillXML.BillType
	// bill.IntroducedAt = billxml.BillXML.IntroducedAt
	// bill.Congress = billxml.BillXML.Congress
	// bill.Summary = struct {
	// 	Date    string
	// 	Text   string
	// }{
	// 	Date: fmt.Sprintf(billxml.BillXML.Summary.XMLBillSummaries.XMLBillItems[0].Date),
	// 	Text: fmt.Sprintf(billxml.BillXML.Summary.XMLBillSummaries.XMLBillItems[0].Text),
	// }

	// var actions []struct{
	// 	ActedAt string `json:"acted_at"`
	// 	Text    string
	// 	Type    string
	// }
	// for i,action := range billxml.BillXML.Actions {
	// 	actions = append(actions, {
	// 		ActedAt: billxml.BillXML.Actions[i].ActedAt,
	// 		Text: billxml.BillXML.Actions[i].Text,
	// 		Type: "", 
	// 	})
	// }
	// bill.Actions = actions
	// bill.Sponsors = billxml.BillXML.Sponsors
	// bill.Cosponsors = billxml.BillXML.Cosponsors
	// bill.StatusAt = billxml.BillXML.Actions[0].actionDate
	// bill.ShortTitle = billxml.BillXML.Title
	// fmt.Printf("%s %s %s %s",bill.BillXML.Number, bill.BillXML.Congress, bill.BillXML.IntroducedAt, bill.BillXML.BillType)
	// ctx := context.Background()
	// dsn := "postgres://postgres:postgres@localhost:5432/csearch?sslmode=disable"
	// // dsn := "unix://user:pass@dbname/var/run/postgresql/.s.PGSQL.5431"
	// sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	// db := bun.NewDB(sqldb, pgdialect.New())
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
			files, err := ioutil.ReadDir(fmt.Sprintf("../congress_api/scraper/congress/data/%s/bills/%s", strconv.Itoa(i), table))
			if err != nil {
				log.Fatal(err)
				continue
			}
			var bills []Bill
			wg.Add(len(files))
			for _, f := range files {
				path := fmt.Sprintf("../congress_api/scraper/congress/data/%s/bills/%s/", strconv.Itoa(i), table) + f.Name()
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
