export class ClientError extends Error {
  constructor(
    message: string,
    public status?: number,
  ) {
    super(message);
    this.name = "ClientError";
  }
}

export class NetworkError extends ClientError {
  constructor(message = "Network request failed") {
    super(message);
    this.name = "NetworkError";
  }
}

export class TimeoutError extends ClientError {
  constructor(message = "Request timed out") {
    super(message, 408);
    this.name = "TimeoutError";
  }
}

export class ServerError extends ClientError {
  constructor(message: string, status: number) {
    super(message, status);
    this.name = "ServerError";
  }
}

export class ValidationError extends ClientError {
  constructor(
    message: string,
    public fieldErrors: Record<string, string[]> = {},
  ) {
    super(message, 422);
    this.name = "ValidationError";
  }
}

/**
 * Factory to parse an error response and throw the appropriate ClientError.
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

  throw new ClientError(message, status);
}
