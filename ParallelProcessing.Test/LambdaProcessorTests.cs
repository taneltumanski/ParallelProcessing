using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
            using (var processor = new ParallelProcessor<int, string>(new StringProcessor(), 1))
            {
                var list = new List<string>();

                var disposable = processor
                    .GetObservable()
                    .Subscribe(x =>
                    {
                        lock (list)
                        {
                            list.Add(x);
                        }
                    });

                using (disposable)
                {
                    processor.ProcessObject(5);

                    SpinWait.SpinUntil(() => list.Count == 1, TimeSpan.FromSeconds(1));

                    Assert.AreEqual(1, list.Count);
                    Assert.AreEqual("5", list.FirstOrDefault());
                }
            }
        }

        [TestMethod]
        public void Processor_Does_Not_Throw_With_Multiple()
        {
            using (var processor = new ParallelProcessor<int, string>(new StringProcessor(), 5))
            {
                var list = new List<string>();

                var disposable = processor
                    .GetObservable()
                    .Subscribe(x =>
                    {
                        lock (list)
                        {
                            list.Add(x);
                        }
                    });

                using (disposable)
                {
                    var items = Enumerable
                        .Range(0, 1111)
                        .ToArray();

                    foreach (var item in items)
                    {
                        processor.ProcessObject(item);
                    }

                    SpinWait.SpinUntil(() => list.Count == 1111, TimeSpan.FromSeconds(1));

                    Assert.AreEqual(1111, list.Count);
                    Assert.IsTrue(items.SequenceEqual(list.Select(x => int.Parse(x)).OrderBy(x => x)));
                }
            }
        }

        [TestMethod]
        public void Processor_Results_Ordered()
        {
            using (var processor = new OrderedParallelProcessor<Temp1, Temp2>(new TempProcessor(), 5))
            {
                var list = new ConcurrentQueue<Temp2>();

                var disposable = processor
                    .GetObservable()
                    .Subscribe(x =>
                    {
                        list.Enqueue(x);
                    });

                using (disposable)
                {
                    var items = new[] { 6, 2, 5, 4, 3, 1 };

                    foreach (var item in items)
                    {
                        processor.ProcessObject(new Temp1() { Test1 = item });
                    }

                    SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(1));

                    Assert.AreEqual(items.Length, list.Count);
                    Assert.IsTrue(items.SequenceEqual(list.Select(x => x.Test2)));
                }
            }
        }

        [TestMethod]
        public void Processor_Results_Ordered2_WaitOnThread()
        {
            using (var processor = new OrderedParallelProcessor<Temp1, Temp2>(new TempProcessor2(), 5))
            {
                var list = new ConcurrentQueue<Temp2>();

                var disposable = processor
                    .GetObservable()
                    .Subscribe(x =>
                    {
                        list.Enqueue(x);
                    });

                using (disposable)
                {
                    var items = new[] { 6, 2, 5, 4, 3, 1 };

                    foreach (var item in items)
                    {
                        processor.ProcessObject(new Temp1() { Test1 = item });
                    }

                    SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(10));

                    Assert.AreEqual(items.Length, list.Count);
                    Assert.IsTrue(items.SequenceEqual(list.Select(x => x.Test2)));
                }
            }
        }

        [TestMethod]
        public void Processor_Results_Not_Ordered2_WaitOnThread()
        {
            using (var processor = new ParallelProcessor<Temp1, Temp2>(new TempProcessor2(), 5))
            {
                var list = new ConcurrentQueue<Temp2>();

                var disposable = processor
                    .GetObservable()
                    .Subscribe(x =>
                    {
                        list.Enqueue(x);
                    });

                using (disposable)
                {
                    var items = new[] { 6, 2, 5, 4, 3, 1 };

                    foreach (var item in items)
                    {
                        processor.ProcessObject(new Temp1() { Test1 = item });
                    }

                    SpinWait.SpinUntil(() => list.Count == items.Length, TimeSpan.FromSeconds(10));

                    Assert.AreEqual(items.Length, list.Count);
                    Assert.IsTrue(!items.SequenceEqual(list.Select(x => x.Test2)));
                }
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

        private class TempProcessor2 : IProcessor<Temp1, Temp2>
        {
            public Temp2 Process(Temp1 input)
            {
                Task.Delay(TimeSpan.FromSeconds(input.Test1)).Wait();

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
