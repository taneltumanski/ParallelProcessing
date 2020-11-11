using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ParallelProcessing.Test
{
    [TestClass]
    public class LambdaProcessorTests
    {
        [TestMethod]
        public void Processor_Does_Not_Throw()
        {
            using (AssertTimer(TimeSpan.FromSeconds(1)))
            using (var processor = new ParallelProcessor<int, string>(new StringProcessor(), 1))
            {
                var list = new List<string>();

                processor.ProcessObject(5, (_, o, __) =>
                {
                    lock (list)
                    {
                        list.Add(o);
                    }
                });

                if (!SpinWait.SpinUntil(() => list.Count == 1, TimeSpan.FromSeconds(5)))
                {
                    Assert.Fail("Processing delay passed");
                }

                Assert.AreEqual(1, list.Count);
                Assert.AreEqual("5", list.FirstOrDefault());
            }
        }

        private IDisposable AssertTimer(TimeSpan timeSpan)
        {
            var sw = Stopwatch.StartNew();

            return Disposable.Create(() =>
            {
                if (sw.Elapsed >= timeSpan)
                {
                    Assert.Fail("Total delay passed: " + timeSpan);
                }
            });
        }

        [TestMethod]
        public void Processor_Does_Not_Throw_With_Multiple()
        {
            using (AssertTimer(TimeSpan.FromSeconds(1)))
            using (var processor = new ParallelProcessor<int, string>(new StringProcessor(), 5))
            {
                var list = new List<string>();
                var items = Enumerable
                    .Range(0, 1111)
                    .ToArray();

                foreach (var item in items)
                {
                    processor.ProcessObject(item, (_, o, __) =>
                    {
                        lock (list)
                        {
                            list.Add(o);
                        }
                    });
                }

                if (!SpinWait.SpinUntil(() => list.Count == 1111, TimeSpan.FromSeconds(5)))
                {
                    Assert.Fail("Processing delay passed");
                }

                Assert.AreEqual(1111, list.Count);
                Assert.IsTrue(items.SequenceEqual(list.Select(x => int.Parse(x)).OrderBy(x => x)));
            }
        }

        [TestMethod]
        public void Processor_Results_Ordered()
        {
            using (AssertTimer(TimeSpan.FromSeconds(1)))
            using (var processor = new ParallelProcessor<Temp1, Temp2>(new TempProcessor(), 5, ThreadPriority.Normal, false, true))
            {
                var list = new ConcurrentQueue<Temp2>();
                var random = new Random();
                var items = Enumerable.Range(0, 1000).OrderBy(x => random.Next()).ToArray();

                foreach (var item in items)
                {
                    processor.ProcessObject(new Temp1() { Test1 = item }, (_, x, __) => list.Enqueue(x));
                }

                if (!SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(5)))
                {
                    Assert.Fail("Processing delay passed");
                }

                Assert.AreEqual(items.Length, list.Count);
                Assert.IsTrue(items.SequenceEqual(list.Select(x => x.Test2)));
            }
        }

        [TestMethod]
        public void Processor_Results_Ordered2_WaitOnThread()
        {
            using (AssertTimer(TimeSpan.FromSeconds(1)))
            using (var processor = new ParallelProcessor<Temp1, Temp2>(new TempProcessor(), 5, ThreadPriority.Normal, false, true))
            {
                var list = new ConcurrentQueue<Temp2>();
                var random = new Random();
                var items = Enumerable.Range(0, 1000).OrderBy(x => random.Next()).ToArray();

                foreach (var item in items)
                {
                    processor.ProcessObject(new Temp1() { Test1 = item }, (_, x, __) => list.Enqueue(x));
                }

                if (!SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(5)))
                {
                    Assert.Fail("Processing delay passed");
                }

                Assert.AreEqual(items.Length, list.Count);
                Assert.IsTrue(items.SequenceEqual(list.Select(x => x.Test2)));
            }
        }

        [TestMethod]
        public void Processor_Results_Not_Ordered2_WaitOnThread()
        {
            using (AssertTimer(TimeSpan.FromSeconds(1)))
            using (var processor = new ParallelProcessor<Temp1, Temp2>(new TempProcessor(), 5))
            {
                var list = new ConcurrentQueue<Temp2>();
                var random = new Random();
                var items = Enumerable.Range(0, 1000).OrderBy(x => random.Next()).ToArray();

                foreach (var item in items)
                {
                    processor.ProcessObject(new Temp1() { Test1 = item }, (_, o, __) =>
                    {
                        list.Enqueue(o);
                    });
                }

                if (!SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(5)))
                {
                    Assert.Fail("Processing delay passed");
                }

                Assert.AreEqual(items.Length, list.Count);
            }
        }

        [TestMethod]
        public void Processor_Builder_Works()
        {
            var list = new ConcurrentQueue<Temp2>();
            var builder = new ParallelProcessorBuilder<Temp1, Temp2>()
                .AsOrdered()
                .WithBlockingAdd()
                .WithProcessor(x => new Temp2() { Test2 = x.Test1 })
                .WithThreadCount(4)
                .WithThreadPriority(ThreadPriority.Highest)
                .ObserveWith(x =>
                {
                    if (x.TryGetResult(out var res))
                    {
                        list.Enqueue(res);
                    }
                });

            using (AssertTimer(TimeSpan.FromSeconds(1)))
            using (var processor = builder.Build())
            {
                var random = new Random();
                var items = Enumerable.Range(0, 100).OrderBy(x => random.Next()).ToArray();

                foreach (var item in items)
                {
                    processor.ProcessObject(new Temp1() { Test1 = item }, null);
                }

                if (!SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(5)))
                {
                    Assert.Fail("Processing delay passed");
                }

                Assert.AreEqual(items.Length, list.Count);
            }
        }

        [TestMethod]
        public void Processor_Switching_Mid_Processing_Works()
        {
            var list = new ConcurrentQueue<Temp2>();
            var builder = new ParallelProcessorBuilder<Temp1, Temp2>()
                .AsOrdered()
                .WithBlockingAdd()
                .WithProcessor(x =>
                {
                    SpinWait.SpinUntil(() => false, 10);
                    return new Temp2() { Test2 = x.Test1 };
                })
                .WithThreadCount(1)
                .WithThreadPriority(ThreadPriority.Highest)
                .ObserveWith(x =>
                {
                    if (x.TryGetResult(out var res))
                    {
                        list.Enqueue(res);
                    }
                });

            var processor = builder.Build().AsUpdatable();

            using var reset1 = new AutoResetEvent(false);
            using var reset2 = new AutoResetEvent(false);
            using var timer = AssertTimer(TimeSpan.FromSeconds(3));

            try
            {
                var random = new Random();
                var items = Enumerable.Range(0, 100).OrderBy(x => random.Next()).ToArray();

                processor.UpdateProcessor(builder.Build());

                Task.Run(() =>
                {
                    reset1.Set();
                    reset2.WaitOne();

                    try
                    {
                        Console.WriteLine($"Updating processor {processor.GetHashCode()}");
                        processor.UpdateProcessor(builder.Build());
                        processor.UpdateProcessor(builder.Build());
                        processor.UpdateProcessor(builder.Build());
                        Console.WriteLine($"New processor {processor.GetHashCode()}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Processor change error: " + e.Message);
                    }
                });

                Task.Run(() =>
                {
                    reset1.WaitOne();

                    for (var i = 0; i < items.Length; i++)
                    {
                        if (i == items.Length / 2)
                        {
                            reset2.Set();
                        }

                        try
                        {
                            Console.WriteLine($"Processing with {processor.GetHashCode()} {i}");
                            processor.ProcessObject(new Temp1() { Test1 = items[i] }, null);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"For loop error at {i}: " + e.Message);
                        }
                    }
                });

                if (!SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(5)))
                {
                    Assert.Fail("Processing delay passed, list length: " + list.Count);
                }

                Assert.AreEqual(items.Length, list.Count);
            }
            finally
            {
                processor.Dispose();
            }
        }

        private class StringProcessor : IProcessor<int, string>
        {
            public string Process(int input) => input.ToString();
        }

        private class TempProcessor : IProcessor<Temp1, Temp2>
        {
            public Temp2 Process(Temp1 input)
            {
                return new Temp2()
                {
                    Test2 = input.Test1
                };
            }
        }

        public class Temp1
        {
            public int Test1 { get; set; }
        }

        public class Temp2
        {
            public int Test2 { get; set; }
        }
    }
}
