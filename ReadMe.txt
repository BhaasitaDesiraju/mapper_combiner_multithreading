To compile the project:
make
To run the project:
./pthreads numOfSlots numOfUsers <input.txt
Example: ./pthreads 10 5 <input.txt

This project consists of:

1) pthreads.c
	- Implementation of the Producer-consumer model between mapper and reducer
	- mapper reads tuples line by line from input.txt and converts tuples from (userId, action, topic) to (userId, topic, score). The result is written to buffer
	- reducer reads from buffer and Groups outputs of same userIds with same topics and outputs the total score
	- outputs to stdout and output.txt without repeating userIds for same topics
	- pthreads are used to create one thread for mapper and multiple threads for reducer
	- mapper thread	waits if there are no available	slots in the buffer and	the	Reducer thread waits if	there are no items in the buffer

2) input.txt, input2.txt, input3.txt
	- contains input tuples

3) output.txt
	- contains output tuples generated

4) Makefile
	- use make compile to compile all files
	- use make clean to clean the generated executable files

4) ReadMe.txt
	- Description of files in the project