export type ColumnDesc = {
  name: string;
  type: string;
  nullable?: boolean;
};

export type DriverError = {
  code: string;
  message: string;
  details?: Record<string, unknown>;
};

export type DriverRequest = {
  id: string;
  driver: string;
  method: string;
  params?: unknown;
};

export type DriverResponse = {
  id: string;
  ok: boolean;
  result?: unknown;
  error?: DriverError;
};

export type StreamChunk = {
  id: string;
  index: number;
  done: boolean;
  data?: Uint8Array;
};

export type DriverMessage = DriverRequest | DriverResponse | StreamChunk;
