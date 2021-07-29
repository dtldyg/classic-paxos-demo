package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	nodeList = map[string][2]string{
		"1": {"127.0.0.1", "9001"},
		"2": {"127.0.0.1", "9002"},
		"3": {"127.0.0.1", "9003"},
	}
	mostVoteNum = len(nodeList)/2 + 1
	myID        string

	leaderID string

	lock  sync.Mutex
	promN string
	curN  string
	curV  string
)

func main() {
	myID = os.Args[1]
	log("myID: %s", myID)

	go server()
	for {
		ticker := time.NewTicker(time.Second * 3)
		<-ticker.C
		log("----- tick -----")
		log("leader: %s", leaderID)
		alive := learn()
		log("alive: %v", alive)
		if len(alive) < mostVoteNum {
			continue
		}
		if leaderID != "" && alive[leaderID] {
			continue
		}
		// 无leader，或leader失联
		n := fmt.Sprintf("%d-%s", time.Now().Unix(), myID)
		v := myID
		promise, maxV, suc := phase1(alive, n)
		log("promise: %v, maxV: %s", promise, maxV)
		if !suc {
			continue
		}
		if maxV != "" {
			v = maxV
		}
		phase2(promise, n, v)
	}
}

func server() {
	http.HandleFunc("/learn", func(w http.ResponseWriter, _ *http.Request) {
		// 返回当前v
		lock.Lock()
		v := curV
		lock.Unlock()
		_, _ = io.WriteString(w, v)
	})
	http.HandleFunc("/phase1", func(w http.ResponseWriter, r *http.Request) {
		// 若当前n小于reqN，返回当前n-v，否则返回空串
		reqN := r.FormValue("n")
		lock.Lock()
		if reqN <= promN {
			return
		}
		promN = reqN
		curNV := fmt.Sprintf("%s,%s", curN, curV)
		lock.Unlock()
		_, _ = io.WriteString(w, curNV)
	})
	http.HandleFunc("/phase2", func(w http.ResponseWriter, r *http.Request) {
		// 尝试批准reqN-V
		reqN := r.FormValue("n")
		reqV := r.FormValue("v")
		lock.Lock()
		if reqN >= promN {
			curN = reqN
			curV = reqV
		}
		lock.Unlock()
	})
	_ = http.ListenAndServe(fmt.Sprintf(":%s", nodeList[myID][1]), nil)
}

// learn ping all node, update leaderID, return alive node id list
func learn() map[string]bool {
	alive := make(map[string]bool)
	learnV := make(map[string]int)
	for id, addr := range nodeList {
		v, suc := get(addr, "learn")
		if !suc {
			// 失联
			continue
		}
		alive[id] = true
		learnV[v]++
	}
	for v, vote := range learnV {
		if vote >= mostVoteNum {
			leaderID = v
			break
		}
	}
	return alive
}

// phase-1
func phase1(alive map[string]bool, n string) ([]string, string, bool) {
	count := 0
	promise := make([]string, 0)
	maxN := ""
	maxV := ""
	for id := range alive {
		maxNV, suc := get(nodeList[id], fmt.Sprintf("phase1?n=%s", n))
		if !suc {
			// 失联
			continue
		}
		if maxNV == "" {
			// 未做出承诺
			continue
		}
		promise = append(promise, id)
		maxNVs := strings.Split(maxNV, ",")
		if maxNVs[0] > maxN {
			maxN = maxNVs[0]
			maxV = maxNVs[1]
		}
		count++
		if count >= mostVoteNum {
			break
		}
	}
	if count < mostVoteNum {
		return nil, "", false
	}
	return promise, maxV, true
}

// phase-2
func phase2(promise []string, n string, v string) {
	for _, id := range promise {
		_, _ = get(nodeList[id], fmt.Sprintf("phase2?n=%s&v=%s", n, v))
	}
}

// get do http GET request
func get(addr [2]string, path string) (string, bool) {
	client := http.Client{Timeout: time.Millisecond * 250}
	resp, err := client.Get(fmt.Sprintf("http://%s:%s/%s", addr[0], addr[1], path))
	if resp != nil {
		defer resp.Body.Close()
	}
	if err == nil {
		b, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			return string(b), true
		}
	}
	return "", false
}

func log(format string, a ...interface{}) {
	fmt.Printf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05.000"), fmt.Sprintf(format, a...))
}
