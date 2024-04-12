package main

import (
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/axiomhq/hyperloglog"
)

const URI = "https://raw.githubusercontent.com/json-iterator/test-data/master/large-file.json"

type Model struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Public    bool   `json:"public"`
	CreatedAt string `json:"created_at"`
	Actor     struct {
		Id     int    `json:"id"`
		Login  string `json:"login"`
		Grav   string `json:"gravatar_id"`
		Url    string `json:"url"`
		Avatar string `json:"avatar_url"`
	} `json:"actor"`
	Repo struct {
		Id   int    `json:"id"`
		Name string `json:"name"`
		Url  string `json:"url"`
	} `json:"repo"`
	Payload struct {
		Action       string `json:"action"`
		Ref          string `json:"ref"`
		RefType      string `json:"ref_type"`
		MasterBranch string `json:"master_branch"`
		Description  string `json:"description"`
		PusherType   string `json:"pusher_type"`
		Head         string `json:"head"`
		Before       string `json:"before"`
		Commits      []struct {
			Sha    string `json:"sha"`
			Author struct {
				Email string `json:"email"`
				Name  string `json:"name"`
			} `json:"author"`
			Message  string `json:"message"`
			Distinct bool   `json:"distinct"`
			Url      string `json:"url"`
		} `json:"commits"`
	} `json:"payload"`
}

func main() {
	hlog := hyperloglog.New()

	client := http.Client{Timeout: 15 * time.Second}

	res, err := client.Get(URI)
	if err != nil {
		log.Fatalf("error fetching data: %v", err)
	}

	var data []Model
	in, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("error fetching data: %v", err)
	}
	if err := json.Unmarshal(in, &data); err != nil {
		log.Fatalf("error fetching data: %v", err)
	}

	count := 0
	for _, d := range data {
		if d.Public {
			hlog.Insert([]byte(d.Id))
			count += 1
		}
	}

	fi, err := os.Create("hlog.gob.out")
	if err != nil {
		log.Fatalf("error fetching data: %v", err)
	}
	defer fi.Close()
	fz := gzip.NewWriter(fi)
	defer fz.Close()
	gb := gob.NewEncoder(fz)
	gb.Encode(hlog)

	log.Println("Estimated cardinality: %d, Counted: %d", hlog.Estimate(), count)

}
