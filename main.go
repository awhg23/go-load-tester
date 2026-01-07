package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Result 结构体：保存单个请求的结果
type Result struct {
	StatusCode int
	Duration   time.Duration
	Error      error
}

// 1. Worker 函数：干苦力的
// jobs：任务通道（只读）
// results：结果通道（只写）
// wg：等待组，通知我干完活了
func worker(id int, url string, jobs <-chan int, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done() //函数结束时，通知等待组我干完活了

	//不断从 jobs 通道里拿任务，直到通道关闭
	for range jobs {
		start := time.Now()
		//发送HTTP请求
		resp, err := http.Get(url)
		duration := time.Since(start)

		res := Result{
			Duration: duration,
			Error:    err,
		}

		if err == nil {
			res.StatusCode = resp.StatusCode
			resp.Body.Close()
		}

		//把结果塞进通道
		results <- res
	}
}

func main() {
	//2. 命令行参数解析
	urlStr := flag.String("u", "", "Targer URL")
	totalRequests := flag.Int("n", 10, "Total number of requests")
	concurrency := flag.Int("c", 2, "Conrcurrency level")
	flag.Parse()

	if *urlStr == "" {
		fmt.Println("Please provide a URL using -u")
		return
	}

	fmt.Printf("Attacking %s with %d requests using %d workers...\n", *urlStr, *totalRequests, *concurrency)

	//3. 初始化通道和等待组
	jobs := make(chan int, *totalRequests)
	results := make(chan Result, *totalRequests)
	var wg sync.WaitGroup

	//4. 启动Workers
	startTotal := time.Now()
	for w := 1; w <= *concurrency; w++ {
		wg.Add(1)
		go worker(w, *urlStr, jobs, results, &wg)

	}

	//5. 发送任务
	for j := 1; j <= *totalRequests; j++ {
		jobs <- j
	}
	close(jobs) // 所有任务发送完了，关闭任务通道

	//6. 等待所有 Worker 完成
	wg.Wait()
	close(results)

	//7. 统计结果
	totalDuration := time.Since(startTotal)
	var successCount, failCount int
	for res := range results {
		if res.Error != nil || res.StatusCode >= 400 {
			failCount++
		} else {
			successCount++
		}
	}

	fmt.Printf("\n--- Results ---\n")
	fmt.Printf("Total time: %v\n", totalDuration)
	fmt.Printf("Success: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failCount)
}
