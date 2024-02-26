# 1️⃣ The One Billion Row Challenge
A fairly portable implementation of [1BRC](https://github.com/gunnarmorling/1brc) in C++20.

Platform requirements:
* Language support: C++20
* Intrinsic support: __m128i, __m256i
* IO support: mmap/munmap or MapViewOfFile/UnmapViewOfFile
* POSIX compatibility: only on Windows

# Building
1. Use CMake to generate the executable: no dependencies are required.
2. Clone the challenge repository and generate actual data file as described in challenge.

# Running
```sh
1brc [file?] [threads?]
```