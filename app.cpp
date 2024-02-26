#ifdef _WIN32
#define NOMINMAX
#define PROT_READ 0
#define MAP_PRIVATE 0
#define MAP_FAILED nullptr
#define stat64 _stat64
#include <Windows.h>
#include <io.h>
#else
#define _LARGE_TIME_API
#include <sys/mman.h>
#endif
#include <chrono>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <thread>
#include <functional>
#include <array>
#include <algorithm>
#include <immintrin.h>
#ifdef _WIN32
void* mmap(void*, size_t length, int, int, int fd, size_t)
{
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable: 4293)
#endif
	const DWORD length_low = (sizeof(size_t) <= sizeof(DWORD)) ? (DWORD)length : (DWORD)(length & 0xFFFFFFFFL);
	const DWORD length_high = (sizeof(size_t) <= sizeof(DWORD)) ? (DWORD)0 : (DWORD)((length >> 32) & 0xFFFFFFFFL);
#ifdef _MSC_VER
#pragma warning(pop)
#endif
	HANDLE fd_handle = (HANDLE)_get_osfhandle(fd);
	if (fd_handle == INVALID_HANDLE_VALUE)
		return MAP_FAILED;

	HANDLE mapping_handle = CreateFileMapping(fd_handle, nullptr, PAGE_READONLY, length_high, length_low, nullptr);
	if (mapping_handle == NULL)
		return MAP_FAILED;

	void* address = MapViewOfFile(mapping_handle, FILE_MAP_READ, 0, 0, length);
	CloseHandle(mapping_handle);
	return address;
}
void munmap(void* address, size_t)
{
	UnmapViewOfFile(address);
}
#endif

struct configuration
{
	std::chrono::microseconds timing;
	const char* path;
	size_t threads;
	bool show_output;
	bool show_timing;
	bool cleanup;

	configuration() : timing(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()))
	{
	}
	void report_timing()
	{
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()) - timing;
		printf("time: %.3f ms\n", (double)duration.count() / 1000.0);
	}
};

struct temperature_metric
{
	const char* name = "\177";
	int64_t sum = 0;
	int64_t count = 1;
	size_t name_size = 0;
	size_t index = std::numeric_limits<size_t>::max();
	int32_t min = std::numeric_limits<int32_t>::max();
	int32_t max = std::numeric_limits<int32_t>::min();
};

struct temperature_report
{
	std::array<temperature_metric, 4096> metrics;
};

__forceinline void calculate_metric(temperature_report* report, const char* key, size_t key_size, const char* value, size_t value_size)
{
	size_t key_hash = 0;
	for (size_t i = 0; i < key_size; i++)
		key_hash = (key_hash * 31) + key[i];

	static __m128i _sp1 = _mm_set_epi32(10, 1, -11, 0);
	static __m128i _sp2 = _mm_set_epi32(100, 10, 1, -111);
	int32_t temperature_negative = (*value - '-') - 1;
	value += *value == '-';

	const int8_t temperature_floating = ('.' - value[1]);
	__m128i _pvn = _mm_set1_epi32(temperature_negative);
	__m128i _pvf = _mm_set1_epi8(temperature_floating);
	__m128i _pv1 = _mm_set_epi32(value[0], value[2], '0', 0);
	__m128i _pv2 = _mm_set_epi32(value[0], value[1], value[3], '0');
	__m128i _pv = _mm_blendv_epi8(_pv1, _pv2, _pvf);
	__m128i _sp = _mm_blendv_epi8(_sp1, _sp2, _pvf);
	_pv = _mm_mullo_epi32(_pv, _sp);
	_pv = _mm_hadd_epi32(_pv, _pv);
	_pv = _mm_hadd_epi32(_pv, _pv);
	_pv = _mm_sign_epi32(_pv, _pvn);

	int32_t temperature = _mm_cvtsi128_si32(_pv);
	while (true)
	{
		size_t index = key_hash & (report->metrics.size() - 1);
		temperature_metric* metric = &report->metrics[index];
		if (!metric->name_size)
		{
			metric->name = key;
			metric->name_size = key_size;
			metric->index = index;
			metric->max = temperature;
			metric->min = temperature;
			metric->sum = temperature;
			break;
		}
		else if (*metric->name == *key && metric->name[metric->name_size - 1] == key[key_size - 1] && metric->name_size == key_size)
		{
			if (metric->max < temperature)
				metric->max = temperature;
			if (metric->min > temperature)
				metric->min = temperature;
			metric->sum += temperature;
			++metric->count;
			break;
		}
		++key_hash;
	}
}
void calculate_chunk(temperature_report* report, char* buffer, size_t size)
{
	const size_t block_width = sizeof(__m256i);
	const size_t block_length = (size / block_width) * block_width;
	static __m256i _kvs = _mm256_set1_epi8(';');
	static __m128i _rls = _mm_set1_epi8('\n');
	static __m128i _ec = _mm_set1_epi64x(0);

	size_t block_offset = 0;
	while (block_offset < block_length)
	{
		char* key = buffer + block_offset;
		size_t key_size = 0, key_block_count = 7;
		for (size_t i = 0; i < key_block_count; i++)
		{
			const size_t offset = i * block_width;
			__m256i _mvb = _mm256_loadu_epi8(key + offset);
			__m256i _cmp = _mm256_cmpeq_epi8(_mvb, _kvs);
			uint32_t _cmps = _mm256_movemask_epi8(_cmp);
			key_size = offset + std::countr_zero(_cmps);
			block_offset += key_size + 1;
			i += _cmps * key_block_count;
		}

		char* value = buffer + block_offset;
		__m128i _mvb = _mm_loadu_epi8(value);
		__m128i _cmp = _mm_cmpeq_epi8(_mvb, _rls);
		uint32_t _cmps = _mm_movemask_epi8(_cmp);
		size_t value_size = std::countr_zero(_cmps);
		block_offset += std::countr_one(_cmps) * block_offset;
		block_offset += value_size + 1;

		calculate_metric(report, key, key_size, value, value_size);
	}

	while (block_offset < size)
	{
		size_t key_size = block_offset;
		char* key = buffer + block_offset;
		while (buffer[block_offset] != ';')
			++block_offset;
		key_size = block_offset - key_size;
		++block_offset;

		size_t value_size = block_offset;
		char* value = buffer + block_offset;
		while (buffer[block_offset] != '\n' && block_offset < size)
			++block_offset;
		value_size = block_offset - value_size;
		++block_offset;

		calculate_metric(report, key, key_size, value, value_size);
	}
}

int main(int argc, char** argv)
{
	configuration config;
	config.path = argc > 1 ? argv[1] : "measurements.txt";
	config.threads = argc > 2 ? atoi(argv[2]) : 24;
	config.show_output = true;
	config.show_timing = true;

	struct stat64 file;
	if (stat64(config.path, &file) != 0 || !file.st_size)
		return -1;

    int Fd = open(config.path, O_RDONLY | O_BINARY);
	if (Fd == -1)
		return -1;

	char* buffer = (char*)mmap(nullptr, file.st_size, PROT_READ, MAP_PRIVATE, Fd, 0);
	if (buffer == MAP_FAILED)
		return -1;

	std::vector<temperature_report> reports;
	reports.resize(config.threads + 1);

	size_t chunk_offset = 0;
	size_t leftover_size = (size_t)file.st_size;
	size_t target_size = leftover_size / config.threads;
	std::vector<std::thread> chunks;
	chunks.reserve(config.threads);

	for (size_t i = 0; i < config.threads; i++)
	{
		char* chunk_buffer = buffer + chunk_offset;
		size_t chunk_size = (i == config.threads - 1 ? leftover_size : target_size);
		bool whitespace = false;
		while (!(whitespace = ((uint8_t)chunk_buffer[chunk_size] == '\n')) && chunk_size < leftover_size)
			++chunk_size;
		if (whitespace)
			++chunk_size;

		chunks.emplace_back(std::bind(calculate_chunk, &reports[i], chunk_buffer, chunk_size));
		leftover_size -= chunk_size;
		chunk_offset += chunk_size;
	}

	for (auto& chunk : chunks)
		chunk.join();

	temperature_report& accumulated_report = reports.back();
	for (size_t i = 0; i < config.threads; i++)
	{
		auto& report = reports[i];
		for (auto& metric : report.metrics)
		{
			if (!metric.name_size)
				continue;

			auto& accumulated_metric = accumulated_report.metrics[metric.index];
			accumulated_metric.name = metric.name;
			accumulated_metric.name_size = metric.name_size;
			if (accumulated_metric.max < metric.max)
				accumulated_metric.max = metric.max;
			if (accumulated_metric.min > metric.min)
				accumulated_metric.min = metric.min;
			accumulated_metric.sum += metric.sum;
			accumulated_metric.count += metric.count;
		}
	}

	if (config.show_output)
	{
		qsort(accumulated_report.metrics.data(), accumulated_report.metrics.size(), sizeof(temperature_metric), [](const void* pointer_a, const void* pointer_b) -> int
		{
			const temperature_metric* a = (const temperature_metric*)pointer_a;
			const temperature_metric* b = (const temperature_metric*)pointer_b;
			return strcmp(a->name, b->name);
		});

		size_t display_buffer_offset = 1;
		char display_buffer[16384];
		*display_buffer = '{';

		for (auto& accumulated_metric : accumulated_report.metrics)
		{
			if (!accumulated_metric.name_size)
				continue;

			char mini_buffer[128];
			double min = (double)accumulated_metric.min / 10.0;
			double avg = ((double)accumulated_metric.sum / (double)accumulated_metric.count) / 10.0;
			double max = (double)accumulated_metric.max / 10.0;
			size_t mini_buffer_size = (size_t)sprintf(mini_buffer, "%.*s=%.1f/%.1f/%.1f, ", (int)accumulated_metric.name_size, accumulated_metric.name, min, avg, max);
			memcpy(display_buffer + display_buffer_offset, mini_buffer, mini_buffer_size);
			display_buffer_offset += mini_buffer_size;
		}

		if (display_buffer_offset < 3)
			display_buffer_offset = 3;

		display_buffer[display_buffer_offset - 2] = '}';
		display_buffer[display_buffer_offset - 1] = 0;
		puts(display_buffer);
	}

	if (config.show_timing)
		config.report_timing();

	if (config.cleanup)
	{
		munmap(buffer, file.st_size);
		close(Fd);
	}
	else
		std::exit(0);

	return 0;
}