# Multiple-Thread-to-Calculate-all-Primes-under-1-Trillion
Using C code to calculate all Primes under 1 Trillion with multiple thread/C语言利用多线程求一万亿以前的所有质数


# Usage 
REQUIRE AT LEAST 93 GiB MEMORY! 
```bash
mkdir primeParallel
cd primeParallel
```
Then, download primeParallel.c and save it to primeParallel
```bash
gcc -o primes primeParallel.c -lm
./primes
```

# Result
There are 37607912018 primes under 1 Trillion, and the last prime

under 1 Trillion is 999999999989.

The primes will be saved to 59 binary files, in which 5 Bytes stored a 

prime.

For example, in binary File 

'LENGTH_IN_BYTES_645623870__FROM_996432412681_TO_999999999989'

the first 5 Bytes stores the prime 996432412681 as E800000009(Hex).

The file name is made up by 3 parts, 

LENGTH_IN_BYTES_XXXXXXXX, it shows the file length in Bytes,

FROM_, it shows the first prime in the file ,

TO_, it shows the last prime in the file.

# Algorithm
Sieve of Eratosthenes for parallel processing.
