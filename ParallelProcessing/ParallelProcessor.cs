using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelProcessing
{
    public class ParallelProcessor : IParallelProcessor, IDisposable
    {
        private readonly Processor[] _processors;
        private readonly Thread _observableThread;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly CancellationToken _cancellationToken;

        private readonly ConcurrentQueue<long> _idQueue = new ConcurrentQueue<long>();
        private readonly ConcurrentDictionary<long, WrappedOutput> _cachedObjects = new ConcurrentDictionary<long, WrappedOutput>();

        private readonly BlockingCollection<WrappedInput> _availableInputs;
        private readonly BlockingCollection<WrappedOutput> _availableOutputs;
        private readonly bool _asOrdered;
        private long _id = 0;

        public ParallelProcessor(int threadCount) : this(threadCount, ThreadPriority.Normal, false, false) { }
        public ParallelProcessor(int threadCount, ThreadPriority threadPriority, bool isBlocking, bool asOrdered)
        {
            if (isBlocking)
            {
                _availableInputs = new BlockingCollection<WrappedInput>(threadCount);
                _availableOutputs = new BlockingCollection<WrappedOutput>(threadCount);
            }
            else
            {
                _availableInputs = new BlockingCollection<WrappedInput>();
                _availableOutputs = new BlockingCollection<WrappedOutput>();
            }

            _asOrdered = asOrdered;
            _cancellationToken = _cts.Token;
            _processors = new Processor[threadCount];

            for (int i = 0; i < _processors.Length; i++)
            {
                _processors[i] = new Processor(threadPriority, _cancellationToken, $"{typeof(ParallelProcessor)}[{i}]", _availableInputs, AddOutput);
            }

            _observableThread = new Thread(ProcessResults);
            _observableThread.Priority = threadPriority;
            _observableThread.IsBackground = true;
            _observableThread.Start();
        }

        public void ProcessObject<TInput, TOutput>(TInput input, Func<TInput, TOutput> processor, Action<TInput, TOutput, Exception> callback)
        {
            AddInput(new WrappedInput(input, Interlocked.Increment(ref _id), i => processor((TInput)i), (i, o, ex) => callback((TInput)i, (TOutput)o, ex)));
        }

        public void Stop()
        {
            _availableInputs.CompleteAdding();
        }

        public bool WaitForCompletion(TimeSpan timeout)
        {
            return SpinWait.SpinUntil(() => _availableInputs.IsCompleted && _availableOutputs.IsCompleted, timeout);
        }

        protected virtual void AddInput(WrappedInput wrappedObject)
        {
            if (_asOrdered)
            {
                _idQueue.Enqueue(wrappedObject.Id);
            }

            _availableInputs.Add(wrappedObject);
        }

        protected virtual void AddOutput(WrappedOutput wrappedObject)
        {
            _availableOutputs.Add(wrappedObject);
        }

        private void ProcessResults()
        {
            try
            {
                while (!_cancellationToken.IsCancellationRequested)
                {
                    if (_availableOutputs.TryTake(out var output, 100, _cancellationToken))
                    {
                        var sent = true;

                        if (_asOrdered)
                        {
                            if (_idQueue.TryPeek(out var nextId) && nextId == output.InputRequest.Id)
                            {
                                _idQueue.TryDequeue(out var _);

                                OnOutput(output);
                            }
                            else
                            {
                                _cachedObjects.TryAdd(output.InputRequest.Id, output);
                                sent = false;
                            }

                            while (sent)
                            {
                                if (_idQueue.TryPeek(out nextId) && _cachedObjects.TryRemove(nextId, out var result))
                                {
                                    _idQueue.TryDequeue(out var _);

                                    OnOutput(result);
                                }
                                else
                                {
                                    sent = false;
                                }
                            }
                        }
                        else
                        {
                            OnOutput(output);
                        }
                    }

                    if (!_availableOutputs.IsAddingCompleted && _availableInputs.IsCompleted && _processors.All(x => x.IsCompleted))
                    {
                        _availableOutputs.CompleteAdding();
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                _availableOutputs.CompleteAdding();
            }
        }

        protected virtual void OnOutput(WrappedOutput output)
        {
            output.InputRequest.Callback?.Invoke(output.InputRequest.InputObject, output.OutputObject, output.Exception);
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
            Stop();
            WaitForCompletion(TimeSpan.FromSeconds(2));

            _cts.Cancel();

            if (_observableThread.IsAlive)
            {
                _observableThread.Join();
            }

            foreach (var p in _processors)
            {
                p.Dispose();
            }

            _availableInputs.Dispose();
            _availableOutputs.Dispose();
            _cts.Dispose();
        }

        protected class Processor : IDisposable
        {
            private readonly Thread _thread;
            private readonly CancellationToken _cancellationToken;
            private readonly BlockingCollection<WrappedInput> _inputs;
            private readonly Action<WrappedOutput> _addOutputAction;

            public bool IsCompleted { get; private set; }

            public Processor(ThreadPriority priority, CancellationToken cancellationToken, string name, BlockingCollection<WrappedInput> inputs, Action<WrappedOutput> addOutputAction)
            {
                _cancellationToken = cancellationToken;
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
                try
                {
                    while (!_inputs.IsCompleted && !_cancellationToken.IsCancellationRequested)
                    {
                        if (_inputs.TryTake(out var input, 100, _cancellationToken))
                        {
                            try
                            {
                                var result = input.Processor(input.InputObject);

                                _addOutputAction(new WrappedOutput(input, result, default));
                            }
                            catch (Exception e)
                            {
                                _addOutputAction(new WrappedOutput(input, default, e));
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { }
                finally
                {
                    IsCompleted = true;
                }
            }

            public void Dispose()
            {
                if (_thread.IsAlive)
                {
                    _thread.Join(TimeSpan.FromSeconds(2));
                }
            }
        }

        protected class WrappedInput
        {
            public readonly long Id;
            public readonly Action<object, object, Exception> Callback;
            public readonly object InputObject;
            public readonly Func<object, object> Processor;

            public WrappedInput(object input, long id, Func<object, object> processor, Action<object, object, Exception> callback)
            {
                Id = id;
                Processor = processor;
                Callback = callback;
                InputObject = input;
            }
        }

        protected class WrappedOutput
        {
            public readonly WrappedInput InputRequest;
            public readonly Exception Exception;
            public readonly object OutputObject;

            public WrappedOutput(WrappedInput input, object output, Exception exception)
            {
                InputRequest = input;
                Exception = exception;
                OutputObject = output;
            }
        }
    }
}
