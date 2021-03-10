package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Contact is an indvidual contact record
type Contact struct {
	NodeID         string    `json:"nodeID"`
	SpaceAvailable bool      `json:"spaceAvailable"`
	LastTimeout    time.Time `json:"lastTimeout"`
	TimeoutRate    float64   `json:"timeoutRate"`
	ResponseTime   float64   `json:"responseTime"`
	Reputation     int       `json:"reputation"`
	LastSeen       time.Time `json:"LastSeen"`
	Address        string    `json:"address"`
	IP             string    `json:"ip"`
	Protocol       string    `json:"protocol"`
	UserAgent      string    `json:"userAgent"`
}

// PagerState maintains state
type PagerState struct {
	nextPage        int
	nextPageMutex   sync.Mutex
	totalRecords    int
	totalPages      int // Total pages processed that contained results.
	totalPagesMutex sync.Mutex
}

// No notion of channels
func _getContacts(page int) []Contact {
	var pageOfContacts []Contact
	response, err := http.Get(fmt.Sprintf("https://api.internxt.com/contacts/?page=%d", page))

	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}

	response.Body.Close()

	err = json.Unmarshal(responseData, &pageOfContacts)
	if err != nil {
		log.Fatal(err)
	}

	return pageOfContacts
}

// I like that this method doesn't interact with any concurrency control structures
// Only channels.
func getContacts(state *PagerState, ch chan Contact) {
	for {
		// Sort of a work producer. We could have instead used a channel to feed pages.
		// But that Producer would have needed to be aware of the progress that each go
		// thread made.
		state.nextPageMutex.Lock()
		page := state.nextPage
		state.nextPage++
		state.nextPageMutex.Unlock()

		contacts := _getContacts(page)
		contactsLen := len(contacts)

		if contactsLen == 0 {
			return
		}

		state.totalPagesMutex.Lock()
		state.totalPages++
		state.totalPagesMutex.Unlock()
		for i := 0; i < contactsLen; i++ {
			ch <- contacts[i]
		}
	}
}

func crawlContacts(dbPath string) {
	concurrency := 3
	state := PagerState{nextPage: 1}
	wg := sync.WaitGroup{}
	ch := make(chan Contact, 500)

	var database *sql.DB = openDB(dbPath)

	epoch := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		// I love this anonymous function. It makes it so getContacts need not know
		// about the concurrency control.
		go func() {
			getContacts(&state, ch)
			wg.Done()
		}()
	}
	// When all the pages are processed, close the channel. The threads all finishing becomes the
	// control mechanism to tell the consumer to stop
	go func() {
		wg.Wait()
		close(ch)
	}()

	/// Stop consuming when the channel is closed.
	for contact := range ch {
		writeContact(database, contact, epoch)
		state.totalRecords++
	}

	fmt.Printf("Processed %d records in %d pages.\n", state.totalRecords, state.totalPages)
}
