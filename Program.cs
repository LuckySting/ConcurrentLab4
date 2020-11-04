using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;

namespace ConcurrentLab4
{
    class Program
    {
        static int totalWords = 0;
        static int totalLetters = 0;
        static int totalWordsLens = 0;
        static ConcurrentDictionary<string, double>  concurrentDict = new ConcurrentDictionary<string, double>();
        static BlockingCollection<string> collection = new BlockingCollection<string>();
        static String readFile(String filepath)
        {
            using (FileStream fstream = File.OpenRead(filepath))
            {
                byte[] array = new byte[fstream.Length];
                fstream.Read(array, 0, array.Length);
                return System.Text.Encoding.Default.GetString(array);
            }
        }

        static void countSync(object o)
        {
            var payload = (object[])o;
            var path = (String)payload[0];
            var input = readFile(path);
            var inps = (Dictionary<string, double>)payload[1];
            foreach (var word in input.Split(" "))
            {
                String trimmedWord = new string(word.Trim(' ').Replace("\n", "").Replace("\r", "").Where(c => !char.IsPunctuation(c)).ToArray());
                if (trimmedWord != "" && trimmedWord.Length > 1)
                {
                    String wordLen = trimmedWord.Length.ToString();
                    if (!inps.ContainsKey(wordLen))
                    {
                        inps.Add(wordLen, 1);
                    }
                    else
                    {
                        inps[wordLen]++;
                    }
                    Interlocked.Increment(ref totalWordsLens);
                    if (!inps.ContainsKey(trimmedWord))
                    {
                        inps.Add(trimmedWord, 1);
                    } else
                    {
                        inps[trimmedWord]++;
                    }
                    Interlocked.Increment(ref totalWords);
                }
                foreach (char letter in trimmedWord)
                {
                    if (!inps.ContainsKey(letter.ToString()))
                    {
                        inps.Add(letter.ToString(), 1);
                    }
                    else
                    {
                        inps[letter.ToString()]++;
                    }
                    Interlocked.Increment(ref totalLetters);
                }
            }

        }

        static void countConcurrent(object o)
        {
            var payload = (object[])o;
            var path = (String)payload[0];
            var input = readFile(path);
            var inps = (ConcurrentDictionary<string, double>)payload[1];
            foreach (var word in input.Split(" "))
            {
                String trimmedWord = new string(word.Where(c => !char.IsPunctuation(c)).ToArray());
                if (trimmedWord != "" && trimmedWord.Length > 1)
                {
                    String wordLen = trimmedWord.Length.ToString();
                    inps.AddOrUpdate(wordLen, 1, (string key, double oldValue) => oldValue + 1);
                    Interlocked.Increment(ref totalWordsLens);
                    inps.AddOrUpdate(trimmedWord, 1, (string key, double oldValue) => oldValue + 1);
                    Interlocked.Increment(ref totalWords);
                }
                foreach (char letter in trimmedWord)
                {
                    inps.AddOrUpdate(letter.ToString(), 1, (string key, double oldValue) => oldValue + 1);
                    Interlocked.Increment(ref totalLetters);
                }
            }

        }

        static void calculateFreq(object o)
        {
            var freq = (IDictionary<string, double>)o;
            foreach(var key in new List<string>(freq.Keys))
            {
                if (key.Length > 1)
                {
                    int r;
                    if (int.TryParse(key, out r))
                    {
                        freq[key] /= totalWordsLens;
                    } else
                    {
                        freq[key] /= totalWords;
                    }
                    
                } else
                {
                    freq[key] /= totalLetters;
                }
            }
        }

        static Dictionary<string, double> mergeDicts(Dictionary<string, double>[] dicts)
        {
            return dicts
                  .SelectMany(d => d)
                  .GroupBy(
                    kvp => kvp.Key,
                    (key, kvps) => new { Key = key, Value = kvps.Sum(kvp => kvp.Value) }
                  )
                  .ToDictionary(x => x.Key, x => x.Value);
        }

        static Dictionary<string, double> oneThreadSyncWorker(String[] paths)
        {
            var dict = new Dictionary<string, double>();
            foreach (var path in paths)
            {
                countSync((object)(new object[] { path, dict }));
            }
            calculateFreq((object)dict);
            totalLetters = 0;
            totalWords = 0;
            totalWordsLens = 0;
            return dict;
        }

        static Dictionary<string, double> multiThreadingLocalBuffersWorker(int workersCount, String[] paths)
        {
            if (workersCount > paths.Length)
            {
                workersCount = paths.Length;
            }
            var dicts = new Dictionary<string, double>[paths.Length];
            var threads = new Thread[paths.Length];
            var i = 0;
            while (i < paths.Length)
            {
                for (int j = 0; j < workersCount; j++)
                {
                    if (i >= paths.Length)
                    {
                        break;
                    }
                    if (threads[j] == null || !threads[j].IsAlive)
                    {
                        threads[j] = new Thread(countSync);
                        dicts[i] = new Dictionary<string, double>();
                        threads[j].Start((object)(new object[] { paths[i], dicts[i] }));
                        i++;
                    }
                        
                }
            }
            for (int j = 0; j < workersCount; j++)
            {
                if (threads[j] != null)
                {
                    threads[j].Join();
                }
            }
            var dict = mergeDicts(dicts);
            calculateFreq((object)dict);
            totalLetters = 0;
            totalWords = 0;
            totalWordsLens = 0;
            return dict;
        }

        static Dictionary<string, double> multiThreadingGlobalBufferWorker(int workersCount, String[] paths)
        {
            if (workersCount > paths.Length)
            {
                workersCount = paths.Length;
            }
            var threads = new Thread[paths.Length];
            var i = 0;
            while (i < paths.Length)
            {
                for (int j = 0; j < workersCount; j++)
                {
                    if (i >= paths.Length)
                    {
                        break;
                    }
                    if (threads[j] == null || !threads[j].IsAlive)
                    {
                        threads[j] = new Thread(countConcurrent);
                        threads[j].Start((object)(new object[] { paths[i], concurrentDict }));
                        i++;
                    }
                }
            }
            for (int j = 0; j < workersCount; j++)
            {
                if (threads[j] != null)
                {
                    threads[j].Join();
                }
            }
            calculateFreq((object)concurrentDict);
            totalLetters = 0;
            totalWords = 0;
            totalWordsLens = 0;
            return concurrentDict.ToDictionary(kvp => kvp.Key, kvp=>kvp.Value);
        }

        static Dictionary<string, double> multiThreadingBlockingWorker(int counersCount, int readersCount, String[] paths)
        {
            if (readersCount > paths.Length)
            {
                readersCount = paths.Length;
            }
            var readers = new Thread[readersCount];
            var counters = new Thread[counersCount];
            var i = 0;
            for (int j = 0; j < counersCount; j++)
            {
                counters[j] = new Thread(() =>
                {
                    while (true)
                    {
                        String word;
                        if (collection.TryTake(out word))
                        {
                            String trimmedWord = new string(word.Where(c => !char.IsPunctuation(c)).ToArray());
                            if (trimmedWord != "" && trimmedWord.Length > 1)
                            {
                                String wordLen = trimmedWord.Length.ToString();
                                concurrentDict.AddOrUpdate(wordLen, 1, (string key, double oldValue) => oldValue + 1);
                                Interlocked.Increment(ref totalWordsLens);
                                concurrentDict.AddOrUpdate(trimmedWord, 1, (string key, double oldValue) => oldValue + 1);
                                Interlocked.Increment(ref totalWords);
                            }
                            foreach (char letter in trimmedWord)
                            {
                                concurrentDict.AddOrUpdate(letter.ToString(), 1, (string key, double oldValue) => oldValue + 1);
                                Interlocked.Increment(ref totalLetters);
                            }
                            if (collection.Count == 0)
                            {
                                break;
                            }
                        }
                        if (collection.Count == 0)
                        {
                            break;
                        }
                    }
                });
            }
            while (i < paths.Length)
            {
                for (int j = 0; j < readersCount; j++)
                {
                    if (i >= paths.Length)
                    {
                        break;
                    }
                    if (readers[j] == null || !readers[j].IsAlive)
                    {
                        readers[j] = new Thread((object o) =>
                        {
                            var text = readFile((String)o);
                            bool b = false;
                            foreach (var word in text.Split(' '))
                            {
                                b = collection.TryAdd(word);
                                while (!b)
                                {
                                    b = collection.TryAdd(word);
                                }
                            }
                        });
                        readers[j].Start(paths[i]);
                        i++;
                    }
                }
            }
            foreach(var counter in counters)
            {
                counter.Start();
            }
            foreach(var reader in readers)
            {
                reader.Join();
            }
            foreach(var counter in counters)
            {
                counter.Join();
            }
            calculateFreq((object)concurrentDict);
            totalLetters = 0;
            totalWords = 0;
            totalWordsLens = 0;
            return concurrentDict.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
        }

        static bool compareWithSync(IDictionary<string, double> operand, String[] paths)
        {
            var t = oneThreadSyncWorker(paths);
            foreach(var kvp in t)
            {
                if (!operand.ContainsKey(kvp.Key))
                {
                    return false;
                }
                if (operand[kvp.Key] != kvp.Value)
                {
                    var badValue = operand[kvp.Key];
                    return false;
                }
            }
            return true;
        }

        static void Main(string[] args)
        {
            var paths = Directory.GetFiles(@"d:\books\");
            var sw = new Stopwatch();
            for (int i = 1; i < 11; i++)
            {
            for (int j = 1; j < 11; j++)
                {
                    sw.Restart();
                    // var res = multiThreadingLocalBuffersWorker(j, paths);
                    // var res = multiThreadingGlobalBufferWorker(j, paths);
                    var res = multiThreadingBlockingWorker(i, j, paths);
                    sw.Stop();
                    concurrentDict = new ConcurrentDictionary<string, double>();
                    collection = new BlockingCollection<string>();
                Console.WriteLine("Counters {0,2}, Readers {1,2}: {2, 6} ms", i, j, Math.Round(sw.Elapsed.TotalMilliseconds));
               //Console.WriteLine("Threads {0,2}: {1, 6} ms", j, Math.Round(sw.Elapsed.TotalMilliseconds));
            }
           }
            // Console.WriteLine(compareWithSync(res, paths));
        }
    }
}
