using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace ParallelProcessing
{
    public class ParallelProcessorBuilder<TInput, TOutput>
    {
        private int _processorCount = Environment.ProcessorCount;
        private ThreadPriority _threadPriority = ThreadPriority.Normal;
        private bool _isBlocking = false;
        private IProcessor<TInput, TOutput> _processor;
        private bool _isOrdered = false;
        private readonly ICollection<IObserver<TOutput>> _observers = new List<IObserver<TOutput>>();

        public ParallelProcessorBuilder<TInput, TOutput> WithProcessorCount(int processorCount)
        {
            if (processorCount < 1)
            {
                throw new ArgumentException($"ProcessorCount must be > 0");
            }

            _processorCount = processorCount;

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

        public ParallelProcessorBuilder<TInput, TOutput> ObserveWith(IObserver<TOutput> observer)
        {
            _observers.Add(observer);

            return this;
        }

        public IParallelProcessor<TInput, TOutput> Build()
        {
            IParallelProcessor<TInput, TOutput> processor;

            if (_isOrdered)
            {
                processor = new OrderedParallelProcessor<TInput, TOutput>(_processor, _processorCount, _threadPriority, _isBlocking);
            }
            else
            {
                processor = new ParallelProcessor<TInput, TOutput>(_processor, _processorCount, _threadPriority, _isBlocking);
            }

            return new BuilderProcessor(processor, _observers);
        }

        private class BuilderProcessor : IParallelProcessor<TInput, TOutput>
        {
            private readonly IParallelProcessor<TInput, TOutput> _processor;
            private readonly IEnumerable<IDisposable> _disposables;

            public BuilderProcessor(IParallelProcessor<TInput, TOutput> processor, IEnumerable<IObserver<TOutput>> observers)
            {
                _processor = processor;

                var observable = _processor.GetObservable();

                _disposables = observers
                    .Select(x => observable.Subscribe(x))
                    .ToArray();
            }

            public IObservable<TOutput> GetObservable()
            {
                return _processor.GetObservable();
            }

            public void ProcessObject(TInput input)
            {
                _processor.ProcessObject(input);
            }

            public void Dispose()
            {
                foreach (var d in _disposables)
                {
                    d.Dispose();
                }

                _processor.Dispose();
            }
        }
    }
}
