using System;
using Microsoft.Extensions.Options;

namespace McStats.Api.Tests;

internal sealed class TestOptionsMonitor<T> : IOptionsMonitor<T>
{
    public T CurrentValue { get; }

    public TestOptionsMonitor(T currentValue)
    {
        CurrentValue = currentValue;
    }

    public T Get(string? name) => CurrentValue;

    public IDisposable? OnChange(Action<T, string?> listener) => null;
}
