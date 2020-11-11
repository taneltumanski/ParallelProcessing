using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;

namespace ParallelProcessing
{
    public class ParallelProcessorBuilder<TInput, TOutput>
    {
        private int _threadCount = Environment.ProcessorCount;
        private ThreadPriority _threadPriority = ThreadPriority.Normal;
        private bool _isBlocking = false;
        private IProcessor<TInput, TOutput> _processor;
        private bool _isOrdered = false;
        private readonly ICollection<IObserver<ProcessResult<TOutput>>> _observers = new List<IObserver<ProcessResult<TOutput>>>();

        public ParallelProcessorBuilder<TInput, TOutput> WithThreadCount(int threadCount)
        {
            if (threadCount < 1)
            {
                throw new ArgumentException($"ThreadCount must be > 0");
            }

            _threadCount = threadCount;

            return this;
        }

        public ParallelProcessorBuilder<TInput, TOutput> WithThreadPriority(ThreadPriority priority)
        {
            _threadPriority = priority;

            return this;
        }

        public ParallelProcessorBuilder<TInput, TOutput> WithBlockingAdd()
        {
            return WithBlockingAdd(true);
        }

        public ParallelProcessorBuilder<TInput, TOutput> WithBlockingAdd(bool isBlocking)
        {
            _isBlocking = isBlocking;

            return this;
        }

        public ParallelProcessorBuilder<TInput, TOutput> WithProcessor(Func<TInput, TOutput> action)
        {
            return WithProcessor(new LambdaProcessor<TInput, TOutput>(action));
        }

        public ParallelProcessorBuilder<TInput, TOutput> WithProcessor(IProcessor<TInput, TOutput> processor)
        {
            _processor = processor;

            return this;
        }

        public ParallelProcessorBuilder<TInput, TOutput> AsOrdered()
        {
            _isOrdered = true;

            return this;
        }

        public ParallelProcessorBuilder<TInput, TOutput> ObserveWith(IObserver<ProcessResult<TOutput>> observer)
        {
            _observers.Add(observer);

            return this;
        }

        public IParallelProcessor<TInput, TOutput> Build()
        {
            var processor = new ParallelProcessor<TInput, TOutput>(_processor, _threadCount, _threadPriority, _isBlocking, _isOrdered);

            if (_observers.Any())
            {
                return new ObservableProcessor<TInput, TOutput>(processor, _observers, true);
            }

            return processor;
        }
    }
}
