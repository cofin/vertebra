export class VertebraError extends Error {
  constructor(
    message: string,
    public status?: number,
  ) {
    super(message);
    this.name = "VertebraError";
  }
}

export class NetworkError extends VertebraError {
  constructor(message = "Network request failed") {
    super(message);
    this.name = "NetworkError";
  }
}

export class TimeoutError extends VertebraError {
  constructor(message = "Request timed out") {
    super(message, 408);
    this.name = "TimeoutError";
  }
}

export class ServerError extends VertebraError {
  constructor(message: string, status: number) {
    super(message, status);
    this.name = "ServerError";
  }
}

export class ValidationError extends VertebraError {
  constructor(
    message: string,
    public fieldErrors: Record<string, string[]> = {},
  ) {
    super(message, 422);
    this.name = "ValidationError";
  }
}

/**
 * Factory to parse an error response and throw the appropriate VertebraError.
 */
export async function throwParsedError(response: Response): Promise<never> {
  const status = response.status;
  let message = "An unexpected error occurred";
  let fieldErrors: Record<string, string[]> = {};

  try {
    const data = await response.json();
    message = data.message || data.error || message;
    fieldErrors = data.field_errors || data.errors || {};
  } catch {
    // If not JSON, use the status text
    message = response.statusText || message;
  }

  if (status === 422) {
    throw new ValidationError(message, fieldErrors);
  }

  if (status >= 500) {
    throw new ServerError(message, status);
  }

  throw new VertebraError(message, status);
}
