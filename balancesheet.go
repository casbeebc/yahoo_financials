/**	
 * Retreives the annual balance sheets data on Yahoo Finance
 *
 * http://finance.yahoo.com/q/bs?s=<stock symbol>&annual
 */
package main

import (
	"fmt"
	"time"
	"database/sql"
	"github.com/lib/pq"
	"log"
	"strings"	
	"github.com/PuerkitoBio/goquery"
)

func getStock(db *sql.DB, symbol string) {
	doc, err := goquery.NewDocument("http://finance.yahoo.com/q/bs?s="+symbol+"&annual") 
	if err != nil {
		log.Fatal(err)
	}
	
	var balanceSheetData = [3]map[string]string{}

	doc.Find("table .yfnc_tabledata1").Each(func(i int, s *goquery.Selection) {

		dataItem := s.Find("td[align='right']")
		
		balanceSheetData[0] = make(map[string]string)
		balanceSheetData[1] = make(map[string]string)
		balanceSheetData[2] = make(map[string]string)
		
		balanceSheetData[0]["symbol"] = symbol
		balanceSheetData[1]["symbol"] = symbol
		balanceSheetData[2]["symbol"] = symbol
		
		var balanceSheetColumnMap = []string{
			"time_period","cash", "short_term_investments","net_receivables","inventory","other_current_assets","total_current_assets",
			"long_term_investments","property_plant_and_equipment","goodwill","intangible_assets","accumulated_amortization",
			"other_assets","deferred_long_term_asset_charges","total_assets","accounts_payable","short_current_long_term_debt",
			"other_current_liabilities","total_current_liabilities","long_term_debt","other_liabilities","deferred_long_term_liability_charges",
			"minority_interest","negative_goodwill","total_liabilities","misc_stock_options_warrants","redeemable_preferred_stock",
			"preferred_stock","common_stock","retained_earnings","treasury_stock","capital_surplus","other_stockholder_equity",
			"total_stockholder_equity","net_tangible_assets"};
		
		var count = 0
		
		for i := range dataItem.Nodes {
			
			dataItemText := strings.TrimSpace(dataItem.Eq(i).Text())
			dataItemText = strings.Replace(dataItemText, ",", "", -1 )
			dataItemText = strings.Replace(dataItemText, "-", "", -1)
			
			if dataItemText != "" {
				balanceSheetData[i%3][balanceSheetColumnMap[(i%3+count)/3]] = dataItemText
			}
			
			if i%3 == 2 {
				count += 3
			}
		}
		
		for _, e := range balanceSheetData {
			commitData(db, e, balanceSheetColumnMap...)
		}
		
	})
}

func commitData(db *sql.DB, data map[string]string, columns ...string) {
	
	var dataitems = []string{}
	columns = append(columns, "symbol")
	for k, v := range columns {
		if k == 0 && len(data[v]) == 0 {
			break
		}
		dataitems = append(dataitems, data[v])
	}
	
	if len(dataitems) > 0 {
		
		fmt.Printf("%v{%d}\n", dataitems, len(dataitems))
		

		txn, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("balancesheet", columns...))
		if err != nil {
			log.Fatal(err)
		}
		
		new := make([]interface{}, len(dataitems))
		for i, v := range dataitems {
			new[i] = v
		}
		
		_, err = stmt.Exec(new...)
		if err != nil {
			log.Fatal(err)
		}
		
		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}
		
		err = stmt.Close()
		if err != nil {
			log.Fatal(err)
		}

		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}
	
}

func main() {
	
	// Assumes a table of stock symbols 
	
	db, err := sql.Open("postgres", "user=username dbname=db sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	
	rows, err := db.Query("SELECT symbol FROM stocks")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
						log.Fatal(err)
		}
		fmt.Printf("%s\n", symbol)
		getStock(db, symbol)
		time.Sleep(time.Second)
	}
	if err := rows.Err(); err != nil {
					log.Fatal(err)
	}
}