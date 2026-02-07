package main

import (
	"bufio"
	"fmt"
	"log"
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

// RingBuffer - Кольцевой буфер
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
		return result
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
	rb.start = 0
	rb.end = 0
	rb.full = false
	return result
}

func main() {
	// Настройка логирования в консоль
	log.SetOutput(os.Stdout)
	log.Println("[SYSTEM] Пайплайн запущен")

	inputChan := make(chan int)
	filteredChan := make(chan int)
	bufferChan := make(chan int)

	var wg sync.WaitGroup

	// 1. Источник данных
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
				log.Println("[SOURCE] Получена команда выхода")
				close(inputChan)
				break
			}
			num, err := strconv.Atoi(text)
			if err != nil {
				log.Printf("[SOURCE] Ошибка ввода: '%s' не является числом\n", text)
				continue
			}
			log.Printf("[SOURCE] Принято число: %d\n", num)
			inputChan <- num
		}
	}()

	// 2. Фильтр отрицательных
	wg.Add(1)
	go func() {
		defer wg.Done()
		for num := range inputChan {
			if num >= 0 {
				log.Printf("[FILTER < 0] Число %d прошло проверку\n", num)
				filteredChan <- num
			} else {
				log.Printf("[FILTER < 0] Число %d отфильтровано (отрицательное)\n", num)
			}
		}
		close(filteredChan)
	}()

	// 3. Фильтр кратных 3 (кроме 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for num := range filteredChan {
			if num != 0 && num%3 == 0 {
				log.Printf("[FILTER %3] Число %d отфильтровано (кратно 3)\n", num)
				continue
			}
			log.Printf("[FILTER %3] Число %d прошло проверку\n", num)
			bufferChan <- num
		}
		close(bufferChan)
	}()

	// 4. Кольцевой буфер и его опустошение
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
					log.Println("[BUFFER] Входной канал закрыт, финальная очистка...")
					items := ringBuffer.GetAll()
					for _, v := range items {
						fmt.Printf("РЕЗУЛЬТАТ: %d\n", v)
					}
					return
				}
				log.Printf("[BUFFER] Число %d добавлено в буфер\n", num)
				ringBuffer.Put(num)
			case <-ticker.C:
				items := ringBuffer.GetAll()
				if len(items) > 0 {
					log.Printf("[BUFFER] Опустошение буфера (%d элементов)\n", len(items))
					for _, v := range items {
						fmt.Printf("РЕЗУЛЬТАТ: %d\n", v)
					}
				}
			}
		}
	}()

	wg.Wait()
	log.Println("[SYSTEM] Пайплайн успешно завершен")
}
