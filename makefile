compile: 
	gcc -o pthreads pthreads.c -pthread

clean: 
	$(RM) pthreads