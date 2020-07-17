CC := g++
CFLAGS := -O3 -Wall -Wextra -Werror -std=c++14
LIBS=-L../libsort/ -lsort -lpthread

radix_sort: main.o tests.o benchmarks.o
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS) 

%.o: %.cpp local.h
	$(CC) -c $< $(CFLAGS)

clean:
	rm -f *.o radix_sort
