using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;

namespace ParallelProcessing
{
    public static class ParallelProcessorExtensions
    {
        public static void ProcessObject<TIn, TOut>(this IParallelProcessor parallelProcessor, TIn input, IProcessor<TIn, TOut> processor, Action<TIn, TOut, Exception> callback)
        {
            parallelProcessor.ProcessObject(input, x => processor.Process(x), callback);
        }

        public static ParallelProcessorBuilder ObserveWith<TIn, TOut>(this ParallelProcessorBuilder builder, Action<ProcessResult<TOut>> action)
        {
            return builder.ObserveWith(Observer.Create(action));
        }

        public static UpdatableParallelProcessor AsUpdatable(this IParallelProcessor processor)
        {
            return new UpdatableParallelProcessor(processor);
        }

        public static Task<TOut> Process<TIn, TOut>(this IParallelProcessor parallelProcessor, TIn input, Func<TIn, TOut> processor)
        {
            var tcs = new TaskCompletionSource<TOut>(TaskCreationOptions.RunContinuationsAsynchronously);

            parallelProcessor.ProcessObject(input, processor, (_, o, ex) =>
            {
                if (ex != null)
                {
                    tcs.TrySetException(ex);
                }
                else
                {
                    tcs.TrySetResult(o);
                }
            });

            return tcs.Task;
        }

        public static IObservable<ProcessResult<TOut>> AsObservable<TIn, TOut>(this IParallelProcessor processor)
        {
            return AsObservable<TIn, TOut>(processor, false);
        }

        public static IObservable<ProcessResult<TOut>> AsObservable<TIn, TOut>(this IParallelProcessor processor, bool disposeProcessor)
        {
            return Observable
                .Using(() => new ObservableProcessor(processor, Enumerable.Empty<IObserver<ProcessResult<object>>>(), disposeProcessor), x => x.GetObservable<TOut>())
                .Publish()
                .RefCount()
                .AsObservable();
        }
    }

    public class ObservableProcessor : IParallelProcessor
    {
        private readonly IParallelProcessor _processor;
        private readonly IDisposable _subscriptions;
        private readonly bool _disposeProcessor;
        private readonly Subject<ProcessResult<object>> _subject = new Subject<ProcessResult<object>>();

        public ObservableProcessor(IParallelProcessor processor, IEnumerable<IObserver<ProcessResult<object>>> observers, bool disposeProcessor)
        {
            _processor = processor;
            _disposeProcessor = disposeProcessor;
            _subscriptions = new CompositeDisposable(observers.Select(x => _subject.Subscribe(x)).ToArray());
        }

        public void ProcessObject<TInput, TOutput>(TInput input, Func<TInput, TOutput> processor, Action<TInput, TOutput, Exception> callback)
        {
            var actualCallback = new Action<TInput, TOutput, Exception>((i, o, ex) =>
            {
                callback?.Invoke(i, o, ex);

                var result = ex == null ? new ProcessResult<object>(o) : new ProcessResult<object>(ex);

                _subject.OnNext(result);
            });

            _processor.ProcessObject(input, processor, actualCallback);
        }

        public IObservable<ProcessResult<TOut>> GetObservable<TOut>()
        {
            return _subject
                .Select(x => x.TryGetResult(out var res) ? res : null)
                .Where(x => x is TOut)
                .Select(x => new ProcessResult<TOut>((TOut)x))
                .AsObservable();
        }

        public void Stop()
        {
            _processor.Stop();
        }

        public bool WaitForCompletion(TimeSpan timeout)
        {
            return _processor.WaitForCompletion(timeout);
        }

        public void Dispose()
        {
            _subscriptions.Dispose();
            _subject.Dispose();
            
            if (_disposeProcessor)
            {
                _processor.Dispose();
            }
        }
    }

    public readonly struct ProcessResult<T>
    {
        private readonly T _result;

        public Exception Exception { get; }

        public ProcessResult(T result)
        {
            _result = result;
            Exception = null;
        }

        public ProcessResult(Exception ex)
        {
            _result = default;
            Exception = ex;
        }

        public bool TryGetResult(out T result)
        {
            result = _result;

            return Exception == null;
        }
    }
}
