package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Generator генерирует последовательность int64 чисел 1,2,3 и т.д. и
// отправляет их в канал ch. После записи в канал для каждого числа
// вызывается функция fn.

func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {

	defer close(ch) // Закрываем канал по завершении функции
	var n int64 = 1 // Задаём первое число последовательности, равное 1

	// Присваиваем циклу лейбл loop1:
loop1:
	for {
		select {
		// При получении сигнала отмены завершаем цикл с лейблом loop1:
		case <-ctx.Done():
			break loop1
		// Отправляем новые числа последовательности каждую итерацию цикла:
		case ch <- n:
			fn(n)
			n++
		}
	}
}

// Worker читает число из канала in и пишет его в канал out.

func Worker(in <-chan int64, out chan<- int64) {

	// Присваиваем циклу лейбл loop2:
loop2:
	for {
		// Читаем число из канала in проверяем, открыт ли он:
		v, ok := <-in
		if !ok {
			// Если канал in закрыт – закрываем канал out:
			close(out)
			// И завершаем цикл с лейблом loop2:
			break loop2
		}

		// Пишем число в канал out:
		out <- v

		// Делаем паузу 1 мс:
		workerCycleDelay := 1 * time.Millisecond
		time.Sleep(workerCycleDelay)
	}
}

func main() {
	chIn := make(chan int64)

	// Создадём контекст с таймаутом и автоматической отменой через 1 секунду:
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel() // Отменяем контекст при завершении main().

	// Для проверки будем считать количество и сумму отправленных чисел:
	var inputSum int64   // Сумма сгенерированных чисел
	var inputCount int64 // Количество сгенерированных чисел

	// Генерируем числа, считая параллельно их количество и сумму:
	go Generator(ctx, chIn, func(i int64) {
		// Сделаем потокобезопасные вычисления:
		atomic.AddInt64(&inputSum, i)
		atomic.AddInt64(&inputCount, 1)
	})

	const NumOut = 20 // Количество обрабатывающих горутин и каналов
	// Записывать числа из chIn будем в слайс каналов outs:
	outs := make([]chan int64, NumOut)

	for i := 0; i < NumOut; i++ {
		// Создаём каналы и для каждого из них вызываем горутину Worker:
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	// Статистику горутин будем класть в слайс amounts:
	amounts := make([]int64, NumOut)
	// Числа из горутин outs[i] будем класть в канал chOut:
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// Собираем числа из каналов outs:
	for i := 0; i < NumOut; i++ {
		wg.Add(1) // Тут добавим в воркгруппу
		go func(in <-chan int64, i int) {
			defer wg.Done() // Тут убавим из воркгруппы по завершении горутины
			for n := range in {
				chOut <- n   // Положим число в канал chOut
				amounts[i]++ // Добавим статистику горутины в amounts
			}
		}(outs[i], i)
	}

	go func() {
		// Ждём завершения работы горутин для outs:
		wg.Wait()
		// Закрываем результирующий канал:
		close(chOut)
	}()

	var count int64 // Количество чисел результирующего канала
	var sum int64   // Сумма чисел результирующего канала

	// Читаем числа из результирующего канала:
	for n := range chOut {
		sum += n // Посчитаем сумму чисел результирующего канала
		count++  // Посчитаем количество чисел резульирующего канала
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	// Проверим результаты:
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
