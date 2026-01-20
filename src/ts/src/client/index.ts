export type ProcedureMap = Record<string, (params: any) => Promise<any>>;

export interface RpcTransport {
  request: (procedureId: string, payload: unknown) => Promise<unknown>;
}

export function createClient<TProcedures extends ProcedureMap>(transport: RpcTransport) {
  return {
    async call<K extends keyof TProcedures>(
      procedureId: K,
      params: Parameters<TProcedures[K]>[0],
    ): Promise<Awaited<ReturnType<TProcedures[K]>>> {
      const result = await transport.request(String(procedureId), params);
      return result as Awaited<ReturnType<TProcedures[K]>>;
    },
  };
}
