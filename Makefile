main: PersistentMultiMap.cpp
	c++ PersistentMultiMap.cpp -fsanitize=address,undefined -std=c++17 -o main
