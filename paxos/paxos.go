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
	// config & discovery
	nodeList = map[string][2]string{
		"1": {"127.0.0.1", "9001"},
		"2": {"127.0.0.1", "9002"},
		"3": {"127.0.0.1", "9003"},
	}
	mostVoteNum = len(nodeList)/2 + 1
	myID        string

	lock  sync.Mutex
	insts = make(map[string]*inst)
)

type inst struct {
	lock  sync.Mutex
	chosV string // 选定的v（learn结果）
	promN string // 承诺的n（阶段1结果）
	currN string // 当前的n（阶段2结果）
	currV string // 当前的v（阶段2结果）
}

func main() {
	myID = os.Args[1]
	acceptor()
	t := "0"
	for !proposer(t) {
		fmt.Println(111)
		time.Sleep(time.Second)
	}
	log(mustGetInst(t).chosV)
	select {}
}

// ========== paxos只负责指定inst的单值一致性工作 ==========
func acceptor() {
	http.HandleFunc("/learner", func(w http.ResponseWriter, r *http.Request) {
		// 返回当前v
		reqT := r.FormValue("t")
		inst := mustGetInst(reqT)

		inst.lock.Lock()
		v := inst.currV
		inst.lock.Unlock()

		_, _ = io.WriteString(w, v)
	})
	http.HandleFunc("/phase1", func(w http.ResponseWriter, r *http.Request) {
		// 若承诺n小于reqN，返回当前n-v并承诺reqN，否则返回空串
		reqT := r.FormValue("t")
		reqN := r.FormValue("n")
		inst := mustGetInst(reqT)

		inst.lock.Lock()
		if reqN <= inst.promN {
			return
		}
		inst.promN = reqN
		curNV := fmt.Sprintf("%s,%s", inst.currN, inst.currV)
		inst.lock.Unlock()

		_, _ = io.WriteString(w, curNV)
	})
	http.HandleFunc("/phase2", func(w http.ResponseWriter, r *http.Request) {
		// 尝试批准reqN-V
		reqT := r.FormValue("t")
		reqN := r.FormValue("n")
		reqV := r.FormValue("v")
		inst := mustGetInst(reqT)

		inst.lock.Lock()
		if reqN >= inst.promN {
			inst.currN = reqN
			inst.currV = reqV
		}
		inst.lock.Unlock()
	})
	go func() {
		_ = http.ListenAndServe(fmt.Sprintf(":%s", nodeList[myID][1]), nil)
	}()
}

func proposer(t string) (suc bool) {
	chosV, alive := learner(t)
	if len(alive) < mostVoteNum {
		return
	}
	if chosV != "" {
		inst := mustGetInst(t)
		inst.chosV = chosV
		suc = true
		return
	}
	n := fmt.Sprintf("%d-%s", time.Now().Unix(), myID)
	v := myID
	p1Suc, usedV, prom := phase1(t, n, v, alive)
	if !p1Suc {
		return
	}
	phase2(t, n, usedV, prom)
	return
}

func learner(t string) (chosV string, alive []string) {
	alive = make([]string, 0)
	learnV := make(map[string]int)
	for id, addr := range nodeList {
		v, suc := httpGet(addr, fmt.Sprintf("learner?t=%s", t))
		if !suc {
			continue
		}
		alive = append(alive, id)
		learnV[v]++
	}
	for v, vote := range learnV {
		if vote >= mostVoteNum {
			chosV = v
			break
		}
	}
	return chosV, alive
}

func phase1(t, n, v string, alive []string) (suc bool, usedV string, prom []string) {
	maxN := ""
	usedV = v
	prom = make([]string, 0)
	for _, id := range alive {
		maxNV, suc := httpGet(nodeList[id], fmt.Sprintf("phase1?t=%s&n=%s", t, n))
		if !suc {
			continue
		}
		if maxNV == "" {
			continue
		}
		prom = append(prom, id)
		maxNVs := strings.Split(maxNV, ",")
		if maxNVs[0] > maxN {
			maxN = maxNVs[0]
			usedV = maxNVs[1]
		}
		if len(prom) >= mostVoteNum {
			break
		}
	}
	if len(prom) < mostVoteNum {
		return
	}
	suc = true
	return
}

func phase2(t, n, v string, prom []string) {
	for _, id := range prom {
		_, _ = httpGet(nodeList[id], fmt.Sprintf("phase2?t=%s&n=%s&v=%s", t, n, v))
	}
}

func httpGet(addr [2]string, path string) (string, bool) {
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

func mustGetInst(t string) *inst {
	lock.Lock()
	defer lock.Unlock()
	if insts[t] == nil {
		insts[t] = &inst{}
	}
	return insts[t]
}

func log(format string, a ...interface{}) {
	fmt.Printf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05.000"), fmt.Sprintf(format, a...))
}
