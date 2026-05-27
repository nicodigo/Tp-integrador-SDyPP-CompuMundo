all: md5_range

md5_range: md5_range.cu md5.cuh
	nvcc -arch=sm_75 md5_range.cu -o md5_range

clean:
	rm -f md5_range
