using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using YamlDotNet.Serialization;

namespace KafkaNet
{
    public static class Dumper
    {
        public static void Dump(string name, object obj)
        {
            Trace.WriteLine(string.Format("--[ {0} ]------------------------", name));
            var stringBuilder = new StringBuilder();
            var serializer = new Serializer(SerializationOptions.EmitDefaults);
            serializer.Serialize(new IndentedTextWriter(new StringWriter(stringBuilder)), new { Type = obj.GetType().Name, Object = obj });
            Trace.WriteLine(stringBuilder);
            Console.WriteLine(stringBuilder);
        }
    }
}
