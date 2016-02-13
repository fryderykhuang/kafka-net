using System;
using System.Collections.Generic;
using System.IO;
using Buffer;

namespace KafkaNet.Common
{
    public class KafkaMessagePacker : IDisposable
    {
        private const int IntegerByteSize = 4;
        private readonly BigEndianBinaryWriter _stream;

        public KafkaMessagePacker()
        {
            _stream = new BigEndianBinaryWriter(new MemoryStream());
            Pack(IntegerByteSize); //pre-allocate space for buffer length
        }

        public KafkaMessagePacker Pack(byte value)
        {
            _stream.Write(value);
            return this;
        }

        public KafkaMessagePacker Pack(Int32 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(Int16 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(Int64 ints)
        {
            _stream.Write(ints);
            return this;
        }

        public KafkaMessagePacker Pack(Slice slice, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(slice, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(byte[] buffer, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(buffer, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(string data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            _stream.Write(data, encoding);
            return this;
        }

        public KafkaMessagePacker Pack(IEnumerable<string> data, StringPrefixEncoding encoding = StringPrefixEncoding.Int32)
        {
            foreach (var item in data)
            {
                _stream.Write(item, encoding);
            }

            return this;
        }

        public byte[] Payload()
        {
            var buffer = new byte[_stream.BaseStream.Length];
            _stream.BaseStream.Position = 0;
            Pack((Int32)(_stream.BaseStream.Length - IntegerByteSize));
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);
            return buffer;
        }

        public byte[] PayloadNoLength()
        {
            var payloadLength = _stream.BaseStream.Length - IntegerByteSize;
            var buffer = new byte[payloadLength];
            _stream.BaseStream.Position = IntegerByteSize;
            _stream.BaseStream.Read(buffer, 0, (int)payloadLength);
            return buffer;
        }

        public byte[] CrcPayload()
        {
            var buffer = new byte[_stream.BaseStream.Length];

            //copy the payload over
            _stream.BaseStream.Position = 0;
            _stream.BaseStream.Read(buffer, 0, (int)_stream.BaseStream.Length);

            //calculate the crc
            var crc = Crc32Provider.ComputeHash(buffer, IntegerByteSize, buffer.Length);
            buffer[0] = (byte)(crc >> 24);
            buffer[1] = (byte)((crc >> 16) & 0xFF);
            buffer[2] = (byte)((crc >> 8) & 0xFF);
            buffer[3] = (byte)(crc & 0xFF);

            return buffer;
        }

        public void Dispose()
        {
            using (_stream) { }
        }
    }
}