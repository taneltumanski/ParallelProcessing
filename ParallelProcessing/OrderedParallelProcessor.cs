using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace ParallelProcessing
{
    public class OrderedParallelProcessor<TInput, TOutput> : ParallelProcessor<TInput, TOutput>
        where TInput : class
        where TOutput : class
    {
        private readonly ConcurrentQueue<Guid> _idQueue = new ConcurrentQueue<Guid>();

        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor) : base(processor) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, bool isBlockingAdd) : base(processor, isBlockingAdd) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, ThreadPriority threadPriority) : base(processor, threadPriority) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, int threadCount) : base(processor, threadCount) { }
        public OrderedParallelProcessor(IProcessor<TInput, TOutput> processor, int threadCount, ThreadPriority threadPriority, bool isBlockingAdd) : base(processor, threadCount, threadPriority, isBlockingAdd) { }

        protected override void AddInput(WrappedObject<TInput> wrappedObject)
        {
            _idQueue.Enqueue(wrappedObject.Id);

            base.AddInput(wrappedObject);
        }

        protected override IObservable<WrappedObject<TOutput>> GetInternalObservable()
        {
            var baseObservable = base.GetInternalObservable();

            return Observable.Create<WrappedObject<TOutput>>(observer =>
            {
                var lockObject = new object();
                var cachedObjects = new ConcurrentDictionary<Guid, WrappedObject<TOutput>>();

                var orderedObserver = Observer
                    .Create<WrappedObject<TOutput>>(next =>
                    {
                        var sent = true;

                        cachedObjects.TryAdd(next.Id, next);

                        do
                        {
                            if (_idQueue.TryPeek(out var nextId) && cachedObjects.TryRemove(nextId, out var result))
                            {
                                _idQueue.TryDequeue(out var _);

                                observer.OnNext(result);
                            }
                            else
                            {
                                sent = false;
                            }
                        } while (sent);
                    }, ex => observer.OnError(ex), () => observer.OnCompleted());

                return baseObservable.Subscribe(orderedObserver);
            });
        }
    }
}
