using System;
using System.Collections.Generic;
using System.Text;

namespace ParallelProcessing
{
    public interface IProcessor<TInput, TOutput>
    {
        TOutput Process(TInput input);
    }
}
