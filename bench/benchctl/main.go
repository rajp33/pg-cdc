package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	neturl "net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: benchctl recv|load|calib|setup-aws")
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "recv":
		runRecv(args)
	case "load":
		runLoad(args)
	case "calib":
		runCalib(args)
	case "setup-aws":
		runSetupAWS(args)
	default:
		log.Fatalf("unknown command %s", cmd)
	}
}

func nowUs() int64 { return time.Now().UnixMicro() }

func awsCfg(endpoint string) aws.Config {
	return aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
		HTTPClient:   &http.Client{Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 512}, Timeout: 60 * time.Second},
	}
}

var (
	reSeq = regexp.MustCompile(`\\?"bench_seq\\?"\s*:\s*\\?"?(-?\d+)`)
	reTs  = regexp.MustCompile(`\\?"bench_ts\\?"\s*:\s*\\?"?(-?\d+)`)
)

type rec struct {
	seq, ts, recv int64
	size          int32
}

type store struct {
	mu       sync.Mutex
	recs     []rec
	distinct map[int64]struct{}
	bad      int64
}

func newStore() *store { return &store{distinct: map[int64]struct{}{}} }

func parseSeqTs(body []byte) (int64, int64, bool) {
	m1 := reSeq.FindSubmatch(body)
	m2 := reTs.FindSubmatch(body)
	if m1 == nil || m2 == nil {
		return 0, 0, false
	}
	seq, _ := strconv.ParseInt(string(m1[1]), 10, 64)
	ts, _ := strconv.ParseInt(string(m2[1]), 10, 64)
	return seq, ts, true
}

func (s *store) add(body []byte, recv int64) {
	seq, ts, ok := parseSeqTs(body)
	if !ok {
		atomic.AddInt64(&s.bad, 1)
		return
	}
	s.mu.Lock()
	s.recs = append(s.recs, rec{seq, ts, recv, int32(len(body))})
	s.distinct[seq] = struct{}{}
	s.mu.Unlock()
}

func (s *store) reset() {
	s.mu.Lock()
	s.recs = nil
	s.distinct = map[int64]struct{}{}
	atomic.StoreInt64(&s.bad, 0)
	s.mu.Unlock()
}

func pct(xs []int64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	i := int(float64(len(xs)-1) * p)
	return float64(xs[i]) / 1000.0
}

func (s *store) summary(expect, minSeq int64) map[string]any {
	s.mu.Lock()
	recs := make([]rec, len(s.recs))
	copy(recs, s.recs)
	bad := atomic.LoadInt64(&s.bad)
	s.mu.Unlock()
	maxSeq := minSeq + expect - 1
	first := map[int64]rec{}
	var inRange, outRange, sizeSum int64
	var firstRecv, lastRecv int64 = 1 << 62, 0
	for _, r := range recs {
		if r.seq < minSeq || r.seq > maxSeq {
			outRange++
			continue
		}
		inRange++
		sizeSum += int64(r.size)
		firstRecv = min(firstRecv, r.recv)
		lastRecv = max(lastRecv, r.recv)
		if f, ok := first[r.seq]; !ok || r.recv < f.recv {
			first[r.seq] = r
		}
	}
	lat := make([]int64, 0, len(first))
	for _, r := range first {
		lat = append(lat, r.recv-r.ts)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	var missing []int64
	var nMissing int64
	for q := minSeq; q <= maxSeq; q++ {
		if _, ok := first[q]; !ok {
			nMissing++
			if len(missing) < 20 {
				missing = append(missing, q)
			}
		}
	}
	out := map[string]any{
		"expect":         expect,
		"min_seq":        minSeq,
		"received":       inRange,
		"distinct":       len(first),
		"duplicates":     inRange - int64(len(first)),
		"missing":        nMissing,
		"missing_sample": missing,
		"out_of_range":   outRange,
		"unparsed":       bad,
		"first_recv_us":  firstRecv,
		"last_recv_us":   lastRecv,
		"avg_body_bytes": 0.0,
		"lat_ms_p50":     pct(lat, 0.50),
		"lat_ms_p95":     pct(lat, 0.95),
		"lat_ms_p99":     pct(lat, 0.99),
		"lat_ms_max":     pct(lat, 1.0),
	}
	if inRange > 0 {
		out["avg_body_bytes"] = float64(sizeSum) / float64(inRange)
	}
	return out
}

func runRecv(args []string) {
	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	listen := fs.String("listen", ":8080", "")
	proxyListen := fs.String("sns-proxy-listen", "", "")
	upstream := fs.String("sns-upstream", "", "")
	fs.Parse(args)
	st := newStore()
	mux := http.NewServeMux()
	mux.HandleFunc("/count", func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		n, d := len(st.recs), len(st.distinct)
		_, w0 := st.distinct[0]
		st.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"received": n, "distinct": d, "warmup_seen": w0})
	})
	mux.HandleFunc("/seen", func(w http.ResponseWriter, r *http.Request) {
		q, _ := strconv.ParseInt(r.URL.Query().Get("seq"), 10, 64)
		st.mu.Lock()
		var at int64
		for _, x := range st.recs {
			if x.seq == q {
				at = x.recv
				break
			}
		}
		st.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"seq": q, "recv_us": at})
	})
	mux.HandleFunc("/summary", func(w http.ResponseWriter, r *http.Request) {
		e, _ := strconv.ParseInt(r.URL.Query().Get("expect"), 10, 64)
		m, _ := strconv.ParseInt(r.URL.Query().Get("min_seq"), 10, 64)
		json.NewEncoder(w).Encode(st.summary(e, m))
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		st.reset()
		resetProxyStats()
		w.Write([]byte("{}\n"))
	})
	mux.HandleFunc("/proxy-stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(proxyStatsSnapshot())
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t := nowUs()
		b, _ := io.ReadAll(r.Body)
		st.add(b, t)
		w.WriteHeader(200)
	})
	if *proxyListen != "" {
		go func() {
			log.Fatal(http.ListenAndServe(*proxyListen, snsProxy(*upstream, st)))
		}()
	}
	log.Printf("recv listening on %s, sns proxy on %q", *listen, *proxyListen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

var (
	reSuccessful = regexp.MustCompile(`(?s)<Successful>(.*?)</Successful>`)
	reID         = regexp.MustCompile(`<Id>([^<]*)</Id>`)
)

var proxyStats struct{ requests, publishBatch, publish, entries, accepted, other atomic.Int64 }

func proxyStatsSnapshot() map[string]int64 {
	return map[string]int64{
		"requests":      proxyStats.requests.Load(),
		"publish_batch": proxyStats.publishBatch.Load(),
		"publish":       proxyStats.publish.Load(),
		"entries":       proxyStats.entries.Load(),
		"accepted":      proxyStats.accepted.Load(),
		"other":         proxyStats.other.Load(),
	}
}

func resetProxyStats() {
	for _, c := range []*atomic.Int64{&proxyStats.requests, &proxyStats.publishBatch, &proxyStats.publish, &proxyStats.entries, &proxyStats.accepted, &proxyStats.other} {
		c.Store(0)
	}
}

func snsProxy(upstream string, st *store) http.Handler {
	client := &http.Client{Transport: &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024}, Timeout: 120 * time.Second}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyStats.requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		req, _ := http.NewRequest(r.Method, upstream+r.URL.RequestURI(), strings.NewReader(string(body)))
		for k, v := range r.Header {
			req.Header[k] = v
		}
		req.Host = r.Host
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		rb, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t := nowUs()
		if resp.StatusCode == 200 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
			form, _ := neturl.ParseQuery(string(body))
			switch form.Get("Action") {
			case "PublishBatch":
				proxyStats.publishBatch.Add(1)
				msgs := map[string]string{}
				for i := 1; i <= 10; i++ {
					id := form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i))
					if id == "" {
						break
					}
					msgs[id] = form.Get(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i))
				}
				proxyStats.entries.Add(int64(len(msgs)))
				if m := reSuccessful.FindSubmatch(rb); m != nil {
					for _, idm := range reID.FindAllSubmatch(m[1], -1) {
						if msg, ok := msgs[string(idm[1])]; ok {
							proxyStats.accepted.Add(1)
							st.add([]byte(msg), t)
						}
					}
				}
			case "Publish":
				proxyStats.publish.Add(1)
				proxyStats.entries.Add(1)
				proxyStats.accepted.Add(1)
				st.add([]byte(form.Get("Message")), t)
			default:
				proxyStats.other.Add(1)
			}
		} else {
			proxyStats.other.Add(1)
		}
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(rb)
	})
}

func payload(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

const insertSQL = "INSERT INTO bench_events (bench_seq, payload) VALUES ($1, $2)"

func insertTxn(ctx context.Context, conn *pgx.Conn, start int64, n int, pay string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	b := &pgx.Batch{}
	for i := 0; i < n; i++ {
		b.Queue(insertSQL, start+int64(i), pay)
	}
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func runLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	dsn := fs.String("dsn", "", "")
	rows := fs.Int64("rows", 0, "")
	startSeq := fs.Int64("start-seq", 1, "")
	txn := fs.Int("txn", 100, "rows per transaction when unpaced")
	conns := fs.Int("conns", 4, "connections when unpaced")
	rate := fs.Int("rate", 0, "rows/s on one connection, committed every 10 ms; 0 = as fast as possible")
	payBytes := fs.Int("payload", 200, "")
	fs.Parse(args)
	ctx := context.Background()
	pay := payload(*payBytes)
	t0 := time.Now()
	if *rate == 0 {
		var next int64
		var wg sync.WaitGroup
		var mu sync.Mutex
		for c := 0; c < *conns; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := pgx.Connect(ctx, *dsn)
				if err != nil {
					log.Fatal(err)
				}
				defer conn.Close(ctx)
				for {
					mu.Lock()
					off := next
					next += int64(*txn)
					mu.Unlock()
					if off >= *rows {
						return
					}
					n := min(int64(*txn), *rows-off)
					if err := insertTxn(ctx, conn, *startSeq+off, int(n), pay); err != nil {
						log.Fatal(err)
					}
				}
			}()
		}
		wg.Wait()
	} else {
		conn, err := pgx.Connect(ctx, *dsn)
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close(ctx)
		var done int64
		tick := 10 * time.Millisecond
		for done < *rows {
			target := min(int64(time.Since(t0).Seconds()*float64(*rate))+1, *rows)
			if target > done {
				if err := insertTxn(ctx, conn, *startSeq+done, int(target-done), pay); err != nil {
					log.Fatal(err)
				}
				done = target
			}
			time.Sleep(tick - time.Since(t0)%tick)
		}
	}
	el := time.Since(t0).Seconds()
	json.NewEncoder(os.Stdout).Encode(map[string]any{"rows": *rows, "start_seq": *startSeq, "seconds": el, "rows_per_s": float64(*rows) / el, "offered_rate": *rate, "txn": *txn, "start_us": t0.UnixMicro(), "end_us": nowUs()})
}

func runSetupAWS(args []string) {
	fs := flag.NewFlagSet("setup-aws", flag.ExitOnError)
	endpoint := fs.String("aws-endpoint", "", "")
	topic := fs.String("topic", "bench", "")
	bucket := fs.String("bucket", "", "")
	fs.Parse(args)
	ctx := context.Background()
	cfg := awsCfg(*endpoint)
	t, err := sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: topic})
	if err != nil {
		log.Fatal(err)
	}
	if *bucket != "" {
		s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
		if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
			log.Fatal(err)
		}
	}
	json.NewEncoder(os.Stdout).Encode(map[string]string{"topic_arn": aws.ToString(t.TopicArn)})
}

func runCalib(args []string) {
	fs := flag.NewFlagSet("calib", flag.ExitOnError)
	mode := fs.String("mode", "sns", "sns|http")
	endpoint := fs.String("aws-endpoint", "", "")
	topic := fs.String("topic-arn", "", "")
	url := fs.String("url", "", "")
	conc := fs.Int("concurrency", 32, "")
	batch := fs.Int("batch", 10, "")
	dur := fs.Duration("duration", 20*time.Second, "")
	rate := fs.Int("rate", 0, "messages/s, 0 = unpaced")
	startSeq := fs.Int64("start-seq", 1, "")
	payBytes := fs.Int("payload", 200, "")
	fs.Parse(args)
	ctx := context.Background()
	pay := payload(*payBytes)
	seq := *startSeq - 1
	var sent, calls, errs int64
	var latMu sync.Mutex
	var callLat []int64
	snc := sns.NewFromConfig(awsCfg(*endpoint))
	hc := &http.Client{Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 512}}
	body := func(q int64) string {
		return fmt.Sprintf(`{"bench_seq":%d,"bench_ts":%d,"payload":"%s"}`, q, nowUs(), pay)
	}
	per := *batch
	if *mode == "http" {
		per = 1
	}
	var limiter <-chan time.Time
	if *rate > 0 {
		tk := time.NewTicker(time.Duration(float64(time.Second) * float64(per) / float64(*rate)))
		defer tk.Stop()
		limiter = tk.C
	}
	deadline := time.Now().Add(*dur)
	t0 := time.Now()
	var wg sync.WaitGroup
	for c := 0; c < *conc; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if limiter != nil {
					<-limiter
				}
				s := time.Now()
				var err error
				if *mode == "sns" {
					entries := make([]snstypes.PublishBatchRequestEntry, per)
					for i := range entries {
						q := atomic.AddInt64(&seq, 1)
						entries[i] = snstypes.PublishBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), Message: aws.String(body(q))}
					}
					var out *sns.PublishBatchOutput
					out, err = snc.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: topic, PublishBatchRequestEntries: entries})
					if err == nil {
						atomic.AddInt64(&sent, int64(len(out.Successful)))
						atomic.AddInt64(&errs, int64(len(out.Failed)))
					}
				} else {
					q := atomic.AddInt64(&seq, 1)
					var resp *http.Response
					resp, err = hc.Post(*url, "application/json", strings.NewReader(body(q)))
					if err == nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						atomic.AddInt64(&sent, 1)
					}
				}
				if err != nil {
					atomic.AddInt64(&errs, 1)
					log.Printf("err: %v", err)
					continue
				}
				atomic.AddInt64(&calls, 1)
				latMu.Lock()
				callLat = append(callLat, time.Since(s).Microseconds())
				latMu.Unlock()
			}
		}()
	}
	wg.Wait()
	el := time.Since(t0).Seconds()
	sort.Slice(callLat, func(i, j int) bool { return callLat[i] < callLat[j] })
	json.NewEncoder(os.Stdout).Encode(map[string]any{
		"mode": *mode, "concurrency": *conc, "batch": per, "offered_rate": *rate, "payload": *payBytes, "seconds": el,
		"messages": sent, "calls": calls, "errors": errs, "messages_per_s": float64(sent) / el, "calls_per_s": float64(calls) / el,
		"call_ms_p50": pct(callLat, 0.5), "call_ms_p99": pct(callLat, 0.99), "start_seq": *startSeq, "last_seq": atomic.LoadInt64(&seq),
	})
}
