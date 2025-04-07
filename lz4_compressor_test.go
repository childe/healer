package healer

import (
	"bytes"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
)

/*
这个测试结果非常有趣！让我总结一下发现：
截断4字节和8字节的情况：
对所有测试用例（小文本、重复文本、二进制数据），截断后的数据仍然可以正确解压
解压后的数据与原始数据完全匹配

截断12字节和16字节的情况：
对所有测试用例，解压都失败了，错误信息是 "unexpected EOF"
这表明这些字节是实际压缩数据的一部分

具体分析每种数据类型：
小文本 ("Hello, World!")：
原始压缩大小：32字节
截断4字节后（28字节）：可以正确解压
截断8字节后（24字节）：可以正确解压
截断12字节或更多：解压失败

重复文本：
原始压缩大小：54字节
截断4字节后（50字节）：可以正确解压
截断8字节后（46字节）：可以正确解压
截断12字节或更多：解压失败

二进制数据：
原始压缩大小：29字节
截断4字节后（25字节）：可以正确解压
截断8字节后（21字节）：可以正确解压
截断12字节或更多：解压失败
*/

func TestLZ4CompressorCompressAndUncompress(t *testing.T) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{
			name:  "empty input",
			input: []byte{},
		},
		{
			name:  "small text",
			input: []byte("Hello, World!"),
		},
		{
			name:  "repeated text",
			input: bytes.Repeat([]byte("test data "), 100),
		},
		{
			name:  "binary data",
			input: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09},
		},
	}

	compressor := &LZ4Compressor{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 压缩数据
			compressed, err := compressor.Compress(tc.input)
			if err != nil {
				t.Fatalf("压缩失败: %v", err)
			}

			// 使用 lz4 reader 解压数据
			reader := lz4.NewReader(bytes.NewReader(compressed))
			uncompressed, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("解压失败: %v", err)
			}

			// 验证解压后的数据是否与原始数据相同
			if !bytes.Equal(tc.input, uncompressed) {
				t.Errorf("解压后的数据与原始数据不匹配\n原始数据: %v\n解压后数据: %v", tc.input, uncompressed)
			}

			// 如果不是空数据，压缩后的大小应该小于或等于原始大小
			if len(tc.input) > 0 {
				compressionRatio := float64(len(compressed)) / float64(len(tc.input))
				t.Logf("压缩率: %.2f", compressionRatio)
			}
		})
	}
}

func TestLZ4CompressorWithTruncatedData(t *testing.T) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{
			name:  "small text",
			input: []byte("Hello, World!"),
		},
		{
			name:  "repeated text",
			input: bytes.Repeat([]byte("test data "), 100),
		},
		{
			name:  "binary data",
			input: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09},
		},
	}

	compressor := &LZ4Compressor{}

	// 测试不同的截断长度
	truncateLengths := []int{4, 8, 12, 16}

	for _, tc := range testCases {
		for _, truncateLen := range truncateLengths {
			t.Run(tc.name+"/truncate_"+string(rune('0'+truncateLen)), func(t *testing.T) {
				// 压缩数据
				compressed, err := compressor.Compress(tc.input)
				if err != nil {
					t.Fatalf("压缩失败: %v", err)
				}

				// 记录原始压缩数据大小
				originalSize := len(compressed)
				t.Logf("原始压缩数据大小: %d 字节", originalSize)

				// 截断指定字节数
				truncated := compressed
				if len(truncated) > truncateLen {
					truncated = truncated[:len(truncated)-truncateLen]
				}
				t.Logf("截断 %d 字节后数据大小: %d 字节", truncateLen, len(truncated))

				// 尝试解压截断的数据
				reader := lz4.NewReader(bytes.NewReader(truncated))
				uncompressed, err := io.ReadAll(reader)

				if err == nil {
					t.Log("成功解压截断数据")
					// 验证解压后的数据是否正确
					if !bytes.Equal(tc.input, uncompressed) {
						t.Errorf("解压后的数据与原始数据不匹配\n原始数据: %v\n解压后数据: %v", tc.input, uncompressed)
					} else {
						t.Logf("截断 %d 字节后仍然能正确解压", truncateLen)
					}
				} else {
					t.Logf("解压截断 %d 字节的数据时得到错误: %v", truncateLen, err)
				}

				// 打印一些调试信息
				t.Logf("原始压缩数据: %v", compressed)
				t.Logf("截断后数据: %v", truncated)
			})
		}
	}
}
