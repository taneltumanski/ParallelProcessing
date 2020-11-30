using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;

namespace ParallelProcessing
{
    public class ParallelProcessorBuilder
    {
        private int _threadCount = Environment.ProcessorCount;
        private ThreadPriority _threadPriority = ThreadPriority.Normal;
        private bool _isBlocking = false;
        private bool _isOrdered = false;

        private readonly ICollection<IObserver<ProcessResult<object>>> _observers = new List<IObserver<ProcessResult<object>>>();

        public ParallelProcessorBuilder WithThreadCount(int threadCount)
        {
            if (threadCount < 1)
            {
                throw new ArgumentException($"ThreadCount must be > 0");
            }

            _threadCount = threadCount;

            return this;
        }

        public ParallelProcessorBuilder WithThreadPriority(ThreadPriority priority)
        {
            _threadPriority = priority;

            return this;
        }

        public ParallelProcessorBuilder WithBlockingAdd()
        {
            return WithBlockingAdd(true);
        }

        public ParallelProcessorBuilder WithBlockingAdd(bool isBlocking)
        {
            _isBlocking = isBlocking;

            return this;
        }

        public ParallelProcessorBuilder AsOrdered()
        {
            _isOrdered = true;

            return this;
        }

        public ParallelProcessorBuilder ObserveWith<TOutput>(IObserver<ProcessResult<TOutput>> observer)
        {
            _observers.Add(Observer.Create<ProcessResult<object>>(x =>
            {
                if (x.Exception != null)
                {
                    observer.OnNext(new ProcessResult<TOutput>(x.Exception));
                }
                else if (x.TryGetResult(out var result) && result is TOutput output)
                {
                    observer.OnNext(new ProcessResult<TOutput>(output));
                }
            },
            x => observer.OnError(x),
            () => observer.OnCompleted()));

            return this;
        }

        public IParallelProcessor Build()
        {
            var processor = new ParallelProcessor(_threadCount, _threadPriority, _isBlocking, _isOrdered);

            if (_observers.Any())
            {
                return new ObservableProcessor(processor, _observers, true);
            }

            return processor;
        }
    }
}
