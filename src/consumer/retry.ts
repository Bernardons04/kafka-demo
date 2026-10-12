type RetryOptions = {
  maxAttempts: number;
  delayMs: number;
};

export async function retry<T>(
  operation: () => Promise<T>,
  { maxAttempts, delayMs }: RetryOptions,
): Promise<T> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (attempt === maxAttempts) {
        throw error;
      }

      console.error(
        `Attempt ${attempt}/${maxAttempts} failed. Retrying in ${delayMs}ms...`,
      );

      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }

  throw new Error("Retry attempts exhausted");
}