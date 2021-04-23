
default: analisi

analisi: analisi.o
	gcc analisi.o -o analisi -lpthread -O

analisi.o: analisi.c
	gcc -c analisi.c 

