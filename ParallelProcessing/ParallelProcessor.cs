using System;
using System.Collections.Concurrent;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelProcessing
{
    public class ParallelProcessor<TInput, TOutput> : IParallelProcessor<TInput, TOutput>, IDisposable
    {
        private readonly Subject<WrappedObject<TOutput>> _subject = new Subject<WrappedObject<TOutput>>();

        private readonly Processor[] _processors;
        private readonly Thread _observableThread;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly CancellationToken _cancellationToken;

        private readonly BlockingCollection<WrappedObject<TInput>> _availableInputs;
        private readonly BlockingCollection<WrappedObject<TOutput>> _availableOutputs;

        private long _id = 0;

        public ParallelProcessor(IProcessor<TInput, TOutput> processor) : this(processor, Environment.ProcessorCount) { }
        public ParallelProcessor(IProcessor<TInput, TOutput> processor, bool isBlocking) : this(processor, Environment.ProcessorCount, ThreadPriority.Normal, isBlocking) { }
        public ParallelProcessor(IProcessor<TInput, TOutput> processor, ThreadPriority threadPriority) : this(processor, Environment.ProcessorCount, threadPriority, false) { }
        public ParallelProcessor(IProcessor<TInput, TOutput> processor, int threadCount) : this(processor, threadCount, ThreadPriority.Normal, false) { }
        public ParallelProcessor(IProcessor<TInput, TOutput> processor, int threadCount, ThreadPriority threadPriority, bool isBlocking)
        {
            if (processor == null)
            {
                throw new ArgumentNullException(nameof(processor));
            }

            if (isBlocking)
            {
                _availableInputs = new BlockingCollection<WrappedObject<TInput>>(threadCount);
                _availableOutputs = new BlockingCollection<WrappedObject<TOutput>>(threadCount);
            }
            else
            {
                _availableInputs = new BlockingCollection<WrappedObject<TInput>>();
                _availableOutputs = new BlockingCollection<WrappedObject<TOutput>>();
            }
            
            _processors = new Processor[threadCount];

            for (int i = 0; i < _processors.Length; i++)
            {
                _processors[i] = new Processor(threadPriority, _cts.Token, $"{typeof(ParallelProcessor<TInput, TOutput>)}[{i}]", processor, _availableInputs, AddOutput);
            }

            _cancellationToken = _cts.Token;

            _observableThread = new Thread(ProcessResults);
            _observableThread.Priority = threadPriority;
            _observableThread.IsBackground = true;
            _observableThread.Start();
        }

        public IObservable<TOutput> GetObservable()
        {
            return GetInternalObservable()
                .Select(x => x.Object)
                .AsObservable();
        }

        public void ProcessObject(TInput input)
        {
            AddInput(new WrappedObject<TInput>(input, Interlocked.Increment(ref _id)));
        }

        protected virtual void AddInput(WrappedObject<TInput> wrappedObject)
        {
            if (_cancellationToken.IsCancellationRequested || _availableInputs.IsAddingCompleted)
            {
                return;
            }

            _availableInputs.Add(wrappedObject);
        }

        protected virtual void AddOutput(WrappedObject<TOutput> wrappedObject)
        {
            if (_cancellationToken.IsCancellationRequested || _availableOutputs.IsAddingCompleted)
            {
                return;
            }

            _availableOutputs.Add(wrappedObject);
        }

        protected virtual IObservable<WrappedObject<TOutput>> GetInternalObservable()
        {
            return _subject.AsObservable();
        }

        private void ProcessResults()
        {
            while (!_cancellationToken.IsCancellationRequested && !_availableOutputs.IsAddingCompleted)
            {
                if (_availableOutputs.TryTake(out var output, TimeSpan.FromSeconds(2)))
                {
                    if (_cancellationToken.IsCancellationRequested || _availableOutputs.IsAddingCompleted)
                    {
                        break;
                    }

                    _subject.OnNext(output);
                }
            }

            _subject.OnCompleted();
        }        

        public void Dispose()
        {
            if (!_cancellationToken.IsCancellationRequested)
            {
                DisposeImpl();
            }
        }

        protected virtual void DisposeImpl()
        {
            _cts.Cancel();

            if (!_availableInputs.IsAddingCompleted)
            {
                _availableInputs.CompleteAdding();
            }

            if (!_availableOutputs.IsAddingCompleted)
            {
                _availableOutputs.CompleteAdding();
            }

            if (_observableThread.IsAlive)
            {
                _observableThread.Join();
            }

            foreach (var p in _processors)
            {
                p.Dispose();
            }

            _subject.Dispose();
            _availableInputs.Dispose();
            _availableOutputs.Dispose();
            _cts.Dispose();
        }

        protected class Processor : IDisposable
        {
            private readonly Thread _thread;
            private readonly CancellationToken _cancellationToken;
            private readonly IProcessor<TInput, TOutput> _processor;
            private readonly BlockingCollection<WrappedObject<TInput>> _inputs;
            private readonly Action<WrappedObject<TOutput>> _addOutputAction;

            public Processor(ThreadPriority priority, CancellationToken cancellationToken, string name, IProcessor<TInput, TOutput> processor, BlockingCollection<WrappedObject<TInput>> inputs, Action<WrappedObject<TOutput>> addOutputAction)
            {
                _cancellationToken = cancellationToken;
                _processor = processor;
                _inputs = inputs;
                _addOutputAction = addOutputAction;

                _thread = new Thread(ProcessLoop);
                _thread.IsBackground = true;
                _thread.Priority = priority;
                _thread.Name = name;
                _thread.Start();
            }

            private void ProcessLoop()
            {
                while (!_cancellationToken.IsCancellationRequested && !_inputs.IsAddingCompleted)
                {
                    if (_inputs.TryTake(out var input, TimeSpan.FromSeconds(2)))
                    {
                        if (_cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        var result = _processor.Process(input.Object);

                        if (_cancellationToken.IsCancellationRequested)
                        {
                            break;
                        }

                        _addOutputAction(new WrappedObject<TOutput>(result, input.Id));
                    }
                }
            }

            public void Dispose()
            {
                _thread.Join(TimeSpan.FromSeconds(2));
            }
        }

        protected readonly struct WrappedObject<T>
        {
            public long Id { get; }
            public T Object { get; }

            public WrappedObject(T input, long id) : this()
            {
                Id = id;
                Object = input;
            }
        }
    }
}
