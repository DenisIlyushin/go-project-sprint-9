package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д.
// и отправляет их в канал ch, вызывая fn для каждого числа.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	defer close(ch)
	var i int64 = 1
	for {
		select {
		case <-ctx.Done(): // Завершаем генерацию при завершении контекста
			return
		case ch <- i: // Отправляем число в канал и вызываем функцию подсчета
			fn(i)
			i++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64) {
	defer close(out)
	for num := range in {
		out <- num                       // Перенаправляем данные в выходной канал
		time.Sleep(1 * time.Millisecond) // небольшая пауза
	}
}

// Merge читает числа из нескольких каналов и пишет их в один канал.
func Merge(chOut chan<- int64, outs []<-chan int64, amounts []int64) {
	var wg sync.WaitGroup
	wg.Add(len(outs)) // Добавляем количество горутин в счетчик

	for i, ch := range outs {
		go func(i int, ch <-chan int64) {
			defer wg.Done() // Уменьшаем счетчик по завершении работы горутины
			for num := range ch {
				chOut <- num // Отправляем число в результирующий канал
				amounts[i]++ // Увеличиваем счетчик чисел в данном канале
			}
		}(i, ch)
	}

	wg.Wait()    // Ожидаем завершения всех горутин
	close(chOut) // Закрываем результирующий канал
}

func main() {
	chIn := make(chan int64)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel() // Гарантируем освобождение ресурсов контекста

	var inputSum int64   // сумма сгенерированных чисел
	var inputCount int64 // количество сгенерированных чисел

	// Запускаем генератор чисел
	go Generator(ctx, chIn, func(i int64) {
		// предотвращаем гонку
		atomic.AddInt64(&inputSum, i)
		atomic.AddInt64(&inputCount, 1)
		//// Ограничиваем генерацию для тестирования
		//if inputCount >= 13087 {
		//	cancel()
		//}
	})

	const NumOut = 15 // количество обработчиков

	outs := make([]chan int64, NumOut)
	outsReadonly := make([]<-chan int64, NumOut) // Создаем слайс с правильным типом

	for i := 0; i < NumOut; i++ {
		outs[i] = make(chan int64)
		outsReadonly[i] = outs[i] // Приводим к типу []<-chan int64
		go Worker(chIn, outs[i])  // Запускаем воркера
	}

	amounts := make([]int64, NumOut) // Храним количество элементов в каждом канале
	chOut := make(chan int64, NumOut)

	// Ждём завершения всех воркеров перед запуском Merge
	go Merge(chOut, outsReadonly, amounts)

	var count int64
	var sum int64

	// Читаем числа из результирующего канала
	for num := range chOut {
		sum += num
		count++
	}

	// Выводим статистику
	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}
