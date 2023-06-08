
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: correctness.o kvstore.o MemTable.o SSTable.o

persistence: persistence.o kvstore.o MemTable.o SSTable.o

clean:
	-rm -f correctness persistence *.o
