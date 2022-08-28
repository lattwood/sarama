package sarama

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	gzip "github.com/yasushi-saito/cloudflare-zlib"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4/v4"
)

var (
	lz4WriterPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewWriter(nil)
		},
	}
)

func compress(cc CompressionCodec, level int, data []byte) ([]byte, error) {
	switch cc {
	case CompressionNone:
		return data, nil
	case CompressionGZIP:
		var (
			err    error
			buf    bytes.Buffer
			writer io.WriteCloser
		)

		switch level {
		case CompressionLevelDefault:
			writer, err = gzip.NewWriter(&buf)
		default:
			writer, err = gzip.NewWriterLevel(&buf, level, 512*1024)
			if err != nil {
				return nil, err
			}
		}
		if _, err := writer.Write(data); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case CompressionSnappy:
		return snappy.Encode(data), nil
	case CompressionLZ4:
		writer := lz4WriterPool.Get().(*lz4.Writer)
		defer lz4WriterPool.Put(writer)

		var buf bytes.Buffer
		writer.Reset(&buf)

		if _, err := writer.Write(data); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case CompressionZSTD:
		return zstdCompress(ZstdEncoderParams{level}, nil, data)
	default:
		return nil, PacketEncodingError{fmt.Sprintf("unsupported compression codec (%d)", cc)}
	}
}
