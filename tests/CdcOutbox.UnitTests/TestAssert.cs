namespace CdcOutbox.UnitTests;

internal static class TestAssert
{
    public static async Task<Exception> AssertThrowsAsync<TException>(Task task)
        where TException : Exception
    {
        try
        {
            await task;
        }
        catch (TException exception)
        {
            return exception;
        }

        throw new Xunit.Sdk.XunitException($"Ожидалось исключение {typeof(TException).Name}.");
    }
}
