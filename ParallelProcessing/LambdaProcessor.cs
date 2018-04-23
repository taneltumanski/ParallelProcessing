using System;
using System.Collections.Generic;
using System.Text;

namespace ParallelProcessing
{
    public class LambdaProcessor<TInput, TOutput> : IProcessor<TInput, TOutput>
    {
        private readonly Func<TInput, TOutput> _action;

        public LambdaProcessor(Func<TInput, TOutput> action)
        {
            _action = action ?? throw new ArgumentNullException(nameof(action));
        }

        public TOutput Process(TInput input)
        {
            return _action(input);
        }
    }
}
