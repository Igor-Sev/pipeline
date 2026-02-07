package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	bufferSize    = 10              // Размер кольцевого буфера
	flushInterval = 2 * time.Second // Интервал опустошения буфера
)

// Кольцевой буфер
type RingBuffer struct {
	data  []int
	start int
	end   int
	full  bool
	mu    sync.Mutex
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		data: make([]int, size),
	}
}

func (rb *RingBuffer) Put(value int) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.data[rb.end] = value
	rb.end = (rb.end + 1) % len(rb.data)
	if rb.full {
		rb.start = (rb.start + 1) % len(rb.data)
	}
	if rb.end == rb.start {
		rb.full = true
	}
}

func (rb *RingBuffer) GetAll() []int {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	var result []int
	if !rb.full && rb.start == rb.end {
		return result // пустой
	}

	if rb.full {
		if rb.end > rb.start {
			result = append(result, rb.data[rb.start:rb.end]...)
		} else {
			result = append(result, rb.data[rb.start:]...)
			result = append(result, rb.data[:rb.end]...)
		}
	} else {
		result = append(result, rb.data[rb.start:rb.end]...)
	}
	// После получения очищаем буфер
	rb.start = 0
	rb.end = 0
	rb.full = false
	return result
}

func main() {
	inputChan := make(chan int)
	filteredChan := make(chan int)
	bufferChan := make(chan int)

	var wg sync.WaitGroup

	// Источник данных
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Введите числа (или 'exit' для завершения):")
		for {
			if !scanner.Scan() {
				break
			}
			text := strings.TrimSpace(scanner.Text())
			if strings.ToLower(text) == "exit" {
				close(inputChan)
				break
			}
			num, err := strconv.Atoi(text)
			if err != nil {
				fmt.Println("Это не число. Попробуйте ещё раз.")
				continue
			}
			inputChan <- num
		}
	}()

	// Фильтр отрицательных
	wg.Add(1)
	go func() {
		defer wg.Done()
		for num := range inputChan {
			if num >= 0 {
				filteredChan <- num
			}
			// отрицательные игнорируем
		}
		close(filteredChan)
	}()

	// Фильтр кратных 3 (кроме 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for num := range filteredChan {
			if num != 0 && num%3 == 0 {
				// пропускаем
				continue
			}
			bufferChan <- num
		}
		close(bufferChan)
	}()

	// Кольцевой буфер и его опустошение
	ringBuffer := NewRingBuffer(bufferSize)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		for {
			select {
			case num, ok := <-bufferChan:
				if !ok {
					// канал закрыт, опустошить буфер
					items := ringBuffer.GetAll()
					if len(items) > 0 {
						for _, v := range items {
							fmt.Printf("Получены данные: %d\n", v)
						}
					}
					return
				}
				ringBuffer.Put(num)
			case <-ticker.C:
				// опустошение буфера
				items := ringBuffer.GetAll()
				if len(items) > 0 {
					for _, v := range items {
						fmt.Printf("Получены данные: %d\n", v)
					}
				}
			}
		}
	}()

	wg.Wait()
	fmt.Println("Конвейер завершен.")
}
