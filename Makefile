randomx-zmq : randomx-zmq.c
		gcc randomx-zmq.c -g -O3 -ldl -lzmq -lczmq -lpthread -lstdc++ -LRandomX/build -lrandomx -o randomx-zmq
