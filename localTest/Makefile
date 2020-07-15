CC := g++
CFLAGS := -O3 -Wall -Wextra -std=c++14
LIBS=-L../libsort/ -lsort

radix_sort: main.o
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS) 

%.o: %.cpp
	$(CC) -c $< $(CFLAGS)

clean:
	rm -f *.o radix_sort
