randomx-zmq : randomx-zmq.c
	gcc randomx-zmq.c -g -O3 -march=native -ldl -lzmq -lczmq -lpthread -lstdc++ -LRandomX/build -lrandomx -o randomx-zmq
