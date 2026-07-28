using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

public class ProbeDispatchProxy : DispatchProxy
{
    public string Mode;
    public object Payload;

    protected override object Invoke(MethodInfo targetMethod, object[] args)
    {
        if (Mode == "application" && targetMethod.Name == "get_Compressor")
            return Payload;
        if (Mode == "compressor" && targetMethod.Name == "Run")
            return YuanhangBridge.RunCompression(args[0]);
        if (targetMethod.ReturnType == typeof(void))
            return null;
        if (targetMethod.ReturnType.IsValueType)
            return Activator.CreateInstance(targetMethod.ReturnType);
        return null;
    }
}

public static class YuanhangBridge
{
    private static string _primaryDir;
    private static string _dependencyDir;
    private static string _snappyDir;
    private static string[] _secrets;

    private static Assembly ResolveAssembly(object sender, ResolveEventArgs args)
    {
        string name = new AssemblyName(args.Name).Name + ".dll";
        foreach (string dir in new[] { _primaryDir, _dependencyDir, _snappyDir })
        {
            string candidate = Path.Combine(dir, name);
            if (File.Exists(candidate))
                return Assembly.LoadFrom(candidate);
        }
        return null;
    }

    private static object CreateProxy(Type interfaceType, string mode, object payload)
    {
        MethodInfo factory = null;
        foreach (MethodInfo method in typeof(DispatchProxy).GetMethods(BindingFlags.Public | BindingFlags.Static))
        {
            if (method.Name == "Create" && method.IsGenericMethodDefinition && method.GetGenericArguments().Length == 2)
            {
                factory = method;
                break;
            }
        }
        if (factory == null)
            throw new MissingMethodException("DispatchProxy.Create<T,TProxy>");
        object proxy = factory.MakeGenericMethod(interfaceType, typeof(ProbeDispatchProxy)).Invoke(null, null);
        ProbeDispatchProxy state = (ProbeDispatchProxy)proxy;
        state.Mode = mode;
        state.Payload = payload;
        return proxy;
    }

    private static byte[] Zlib(byte[] source, int index, int count, bool decompress)
    {
        if (count <= 0)
            count = source.Length - index;
        using (MemoryStream input = new MemoryStream(source, index, count, false))
        using (MemoryStream output = new MemoryStream())
        {
            if (decompress)
            {
                using (ZLibStream stream = new ZLibStream(input, CompressionMode.Decompress, false))
                    stream.CopyTo(output);
            }
            else
            {
                using (ZLibStream stream = new ZLibStream(output, CompressionLevel.Optimal, true))
                    input.CopyTo(stream);
            }
            return output.ToArray();
        }
    }

    private static byte[] Snappy(byte[] source, int index, int count, bool decompress)
    {
        Assembly assembly = Assembly.LoadFrom(Path.Combine(_snappyDir, "Hevo.Snappy.dll"));
        if (decompress)
        {
            Type type = assembly.GetType("Snappy.SnappyDecompressor", true);
            object instance = Activator.CreateInstance(type);
            MethodInfo method = type.GetMethod("Decompress", new[] { typeof(byte[]), typeof(int), typeof(int) });
            return (byte[])method.Invoke(instance, new object[] { source, index, count });
        }
        Type helper = assembly.GetType("Snappy.Snappy", true);
        MethodInfo compress = helper.GetMethod("Compress", new[] { typeof(byte[]) });
        if (index == 0 && (count <= 0 || count == source.Length))
            return (byte[])compress.Invoke(null, new object[] { source });
        if (count <= 0)
            count = source.Length - index;
        byte[] slice = new byte[count];
        Buffer.BlockCopy(source, index, slice, 0, count);
        return (byte[])compress.Invoke(null, new object[] { slice });
    }

    public static byte[] RunCompression(object parameter)
    {
        Type parameterType = parameter.GetType();
        string action = Convert.ToString(parameterType.GetProperty("Action").GetValue(parameter, null));
        string compressionType = Convert.ToString(parameterType.GetProperty("CompressType").GetValue(parameter, null));
        object flow = parameterType.GetProperty("Flow").GetValue(parameter, null);
        Type flowType = flow.GetType();
        byte[] source = (byte[])flowType.GetProperty("SourceBytes").GetValue(flow, null);
        int index = Convert.ToInt32(flowType.GetProperty("StartIndex").GetValue(flow, null));
        int count = Convert.ToInt32(flowType.GetProperty("Count").GetValue(flow, null));
        bool decompress = action == "Decompress";
        if (compressionType == "Zlib")
            return Zlib(source, index, count, decompress);
        if (compressionType == "Snappy")
            return Snappy(source, index, count, decompress);
        throw new NotSupportedException("unsupported compression type: " + compressionType);
    }

    private static void InstallCompressionAbility()
    {
        Assembly interfaces = Assembly.Load("Hevo.Core.Interfaces");
        Type compressorInterface = interfaces.GetType("Hevo.Core.Interfaces.ICompressor", true);
        Type applicationInterface = interfaces.GetType("Hevo.Core.Interfaces.IApplicationAbility", true);
        object compressor = CreateProxy(compressorInterface, "compressor", null);
        object application = CreateProxy(applicationInterface, "application", compressor);
        Type controller = interfaces.GetType("Hevo.Core.Interfaces.AppAbilityControler", true);
        controller.GetProperty("ApplicationAbility", BindingFlags.Public | BindingFlags.Static).SetValue(null, application, null);
    }

    private static string Scrub(string value)
    {
        string result = value ?? String.Empty;
        foreach (string secret in _secrets)
        {
            if (!String.IsNullOrEmpty(secret))
                result = result.Replace(secret, "<redacted>");
        }
        result = result.Replace("\r", " ").Replace("\n", " ");
        return result.Length <= 500 ? result : result.Substring(0, 500);
    }

    private static Exception Unwrap(Exception error)
    {
        Exception current = error;
        while (current.InnerException != null)
        {
            if (current is AggregateException)
            {
                AggregateException aggregate = (AggregateException)current;
                aggregate = aggregate.Flatten();
                if (aggregate.InnerExceptions.Count != 1)
                    return aggregate;
                current = aggregate.InnerExceptions[0];
            }
            else
                current = current.InnerException;
        }
        return current;
    }

    private static bool ReadBool(object instance, Type type, string propertyName)
    {
        PropertyInfo property = type.GetProperty(propertyName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        return property != null && Convert.ToBoolean(property.GetValue(instance, null));
    }

    private static object GetDataCenter(Assembly assembly)
    {
        Type dataCenterType = assembly.GetType("Hevo.Core.DataCenter", true);
        return dataCenterType.GetProperty("Current", BindingFlags.Public | BindingFlags.Static).GetValue(null, null);
    }

    private static MethodInfo FindRequestV2(Assembly assembly)
    {
        Type extensionType = assembly.GetType("Hevo.Core.DataCenterExtension", true);
        foreach (MethodInfo method in extensionType.GetMethods(BindingFlags.Public | BindingFlags.Static))
        {
            if (method.Name == "RequestV2" && method.GetParameters().Length == 4)
                return method;
        }
        throw new MissingMethodException("Hevo.Core.DataCenterExtension.RequestV2");
    }

    private static Dictionary<string, object> RunQuery(Assembly assembly, string request)
    {
        DateTime started = DateTime.UtcNow;
        try
        {
            MethodInfo requestMethod = FindRequestV2(assembly);
            object current = GetDataCenter(assembly);
            Task task = (Task)requestMethod.Invoke(null, new object[] { current, request, String.Empty, null });
            if (!task.Wait(TimeSpan.FromSeconds(30)))
                return Error("Timeout", "request exceeded 30 seconds", started);
            object response = task.GetType().GetProperty("Result").GetValue(task, null);
            if (response == null)
                return Error("NullResponse", "remote returned a null response", started);
            Type responseType = response.GetType();
            bool isBad = Convert.ToBoolean(responseType.GetProperty("IsBad").GetValue(response, null));
            bool isString = Convert.ToBoolean(responseType.GetProperty("IsString").GetValue(response, null));
            int size = Convert.ToInt32(responseType.GetProperty("Size").GetValue(response, null));
            string message = String.Empty;
            if (isString)
                message = Scrub(Convert.ToString(responseType.GetMethod("AsString", BindingFlags.Public | BindingFlags.Instance).Invoke(response, null)));
            if (isBad)
                return Error("EmptyResponse", message.Length == 0 ? "remote returned an empty response" : message, started);

            object rawRows = responseType.GetMethod("AsListDictionary", BindingFlags.Public | BindingFlags.Instance).Invoke(response, null);
            List<Dictionary<string, string>> rows = new List<Dictionary<string, string>>();
            IEnumerable sequence = rawRows as IEnumerable;
            if (sequence != null)
            {
                foreach (object rawRow in sequence)
                {
                    IDictionary dictionary = rawRow as IDictionary;
                    if (dictionary == null)
                        continue;
                    Dictionary<string, string> row = new Dictionary<string, string>();
                    foreach (object key in dictionary.Keys)
                    {
                        string field = key == null ? String.Empty : key.ToString();
                        if (field.StartsWith("Field_", StringComparison.Ordinal))
                            field = field.Substring(6);
                        object rawValue = dictionary[key];
                        string value = rawValue == null ? null : rawValue.ToString();
                        if (value == "--" || String.Equals(value, "NaN", StringComparison.OrdinalIgnoreCase))
                            value = null;
                        row[field] = value;
                    }
                    rows.Add(row);
                }
            }
            return new Dictionary<string, object>
            {
                { "type", "response" }, { "ok", true }, { "rows", rows },
                { "size", size }, { "elapsed_ms", Convert.ToInt64((DateTime.UtcNow - started).TotalMilliseconds) }
            };
        }
        catch (Exception rawError)
        {
            Exception error = Unwrap(rawError);
            return Error(error.GetType().FullName, error.Message, started);
        }
    }

    private static Dictionary<string, object> Error(string errorType, string message, DateTime started)
    {
        return new Dictionary<string, object>
        {
            { "type", "response" }, { "ok", false }, { "error_type", errorType },
            { "error", Scrub(message) }, { "rows", new List<object>() },
            { "elapsed_ms", Convert.ToInt64((DateTime.UtcNow - started).TotalMilliseconds) }
        };
    }

    private static void WriteJson(object value)
    {
        Console.WriteLine(JsonSerializer.Serialize(value));
        Console.Out.Flush();
    }

    public static int Main(string[] args)
    {
        string username = Environment.GetEnvironmentVariable("THS_USERNAME") ?? String.Empty;
        string password = Environment.GetEnvironmentVariable("THS_PASSWORD") ?? String.Empty;
        string mac = Environment.GetEnvironmentVariable("THS_MAC") ?? String.Empty;
        _secrets = new[] { username, password, mac };
        _primaryDir = Environment.GetEnvironmentVariable("YUANHANG_PRIMARY_DIR") ?? String.Empty;
        _dependencyDir = Environment.GetEnvironmentVariable("YUANHANG_DEP_DIR") ?? String.Empty;
        _snappyDir = Environment.GetEnvironmentVariable("YUANHANG_SNAPPY_DIR") ?? String.Empty;

        Console.OutputEncoding = new UTF8Encoding(false);
        bool envComplete = username.Length > 0 && password.Length > 0;
        if (!envComplete || _primaryDir.Length == 0 || _dependencyDir.Length == 0 || _snappyDir.Length == 0)
        {
            WriteJson(Error("ConfigurationError", "credentials or library directories are missing", DateTime.UtcNow));
            return 2;
        }

        AppDomain.CurrentDomain.AssemblyResolve += ResolveAssembly;
        Assembly assembly = null;
        try
        {
            assembly = Assembly.LoadFrom(Path.Combine(_primaryDir, "Hevo.Api.Quotes.dll"));
            Type loginType = assembly.GetType("Hevo.Api.Quotes.LoginModel", true);
            object login = Activator.CreateInstance(loginType, new object[] { username, password });
            Task task = (Task)loginType.GetMethod("Start", BindingFlags.Public | BindingFlags.Instance).Invoke(login, null);
            if (!task.Wait(TimeSpan.FromSeconds(45)))
            {
                WriteJson(Error("Timeout", "login exceeded 45 seconds", DateTime.UtcNow));
                return 3;
            }
            InstallCompressionAbility();
            Type dataCenterType = assembly.GetType("Hevo.Core.DataCenter", true);
            object current = GetDataCenter(assembly);
            object token = dataCenterType.GetProperty("UserToken", BindingFlags.Public | BindingFlags.Instance).GetValue(current, null);
            bool online = ReadBool(current, dataCenterType, "IsM_hqMainOnlined");
            if (token == null || !online)
                throw new InvalidOperationException("login completed without an online main quote session");
            WriteJson(new Dictionary<string, object>
            {
                { "type", "ready" }, { "ok", true }, { "version", assembly.GetName().Version.ToString() },
                { "token_present", true }, { "main_online", true }
            });

            string line;
            while ((line = Console.ReadLine()) != null)
            {
                try
                {
                    using (JsonDocument document = JsonDocument.Parse(line))
                    {
                        JsonElement root = document.RootElement;
                        string op = root.GetProperty("op").GetString();
                        if (op == "shutdown")
                        {
                            WriteJson(new Dictionary<string, object> { { "type", "bye" }, { "ok", true } });
                            break;
                        }
                        if (op != "query")
                        {
                            WriteJson(Error("ProtocolError", "unsupported operation", DateTime.UtcNow));
                            continue;
                        }
                        string request = root.GetProperty("request").GetString();
                        WriteJson(RunQuery(assembly, request));
                    }
                }
                catch (Exception requestError)
                {
                    Exception error = Unwrap(requestError);
                    WriteJson(Error(error.GetType().FullName, error.Message, DateTime.UtcNow));
                }
            }
            return 0;
        }
        catch (Exception rawError)
        {
            Exception error = Unwrap(rawError);
            WriteJson(Error(error.GetType().FullName, error.Message, DateTime.UtcNow));
            return 1;
        }
    }
}
