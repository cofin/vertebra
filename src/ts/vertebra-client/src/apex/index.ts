export interface ApexItemHandle {
  getValue(): string;
  setValue(value: string, displayValue?: string, suppressChangeEvent?: boolean): void;
}

export interface ApexGlobalNamespace {
  item(itemName: string): ApexItemHandle;
  event?: {
    trigger(target: unknown, eventName: string, data?: unknown): void;
  };
}

export interface ApexRegionBridgeOptions {
  regionId: string;
  appId: number;
  pageId: number;
  sessionId: number;
  apexGlobal?: ApexGlobalNamespace;
}

export interface IslandDescriptor {
  tagName: string;
  componentName: string;
  regionId?: string;
  liveChannel?: string;
  boundItems?: string[];
  props: Record<string, unknown>;
}

export type ApexOfflineOperation = "set_item" | "row_upsert" | "row_delete" | "duality_patch";

export interface ApexOfflineMutationEntry {
  mutationId: string;
  regionId: string;
  operation: ApexOfflineOperation;
  hlcTimestamp: number;
  itemName?: string;
  value?: unknown;
  etag?: string;
}

export class ApexOfflineOutbox {
  private readonly maxCapacity: number;
  private readonly queue: ApexOfflineMutationEntry[] = [];
  private logicalCounter = 0;
  private lastWallMs = 0;

  constructor(maxCapacity = 512) {
    this.maxCapacity = Math.max(1, maxCapacity);
  }

  get size(): number {
    return this.queue.length;
  }

  private nextHlc(nowMs = Date.now()): number {
    if (nowMs > this.lastWallMs) {
      this.lastWallMs = nowMs;
      this.logicalCounter = 0;
    } else {
      this.logicalCounter = Math.min(0xffff, this.logicalCounter + 1);
    }
    return this.lastWallMs * 65536 + this.logicalCounter;
  }

  private pushEntry(entry: ApexOfflineMutationEntry, replaceIdx = -1): ApexOfflineMutationEntry {
    if (replaceIdx >= 0) {
      this.queue.splice(replaceIdx, 1);
    } else if (this.queue.length >= this.maxCapacity) {
      this.queue.shift();
    }
    this.queue.push(entry);
    return entry;
  }

  enqueueItemMutation(
    regionId: string,
    itemName: string,
    value: string,
    options?: { mutationId?: string; etag?: string; nowMs?: number },
  ): ApexOfflineMutationEntry {
    const hlcTimestamp = this.nextHlc(options?.nowMs);
    const existingIdx = this.queue.findIndex(
      (entry) => entry.operation === "set_item" && entry.regionId === regionId && entry.itemName === itemName,
    );
    return this.pushEntry(
      {
        mutationId: options?.mutationId ?? `${regionId}:${itemName}:${hlcTimestamp}`,
        regionId,
        operation: "set_item",
        hlcTimestamp,
        itemName,
        value,
        ...(options?.etag !== undefined ? { etag: options.etag } : {}),
      },
      existingIdx,
    );
  }

  enqueueRowMutation(
    regionId: string,
    operation: Exclude<ApexOfflineOperation, "set_item">,
    value: unknown,
    options?: { mutationId?: string; etag?: string; nowMs?: number },
  ): ApexOfflineMutationEntry {
    const hlcTimestamp = this.nextHlc(options?.nowMs);
    return this.pushEntry({
      mutationId: options?.mutationId ?? `${regionId}:${operation}:${hlcTimestamp}`,
      regionId,
      operation,
      hlcTimestamp,
      value,
      ...(options?.etag !== undefined ? { etag: options.etag } : {}),
    });
  }

  drain(): ApexOfflineMutationEntry[] {
    return this.queue.splice(0, this.queue.length);
  }
}

export class ApexRegion {
  readonly regionId: string;
  readonly appId: number;
  readonly pageId: number;
  readonly sessionId: number;
  readonly outbox: ApexOfflineOutbox;
  private readonly apexGlobal?: ApexGlobalNamespace;
  private readonly boundItems = new Map<string, string>();

  constructor(options: ApexRegionBridgeOptions) {
    this.regionId = options.regionId;
    this.appId = options.appId;
    this.pageId = options.pageId;
    this.sessionId = options.sessionId;
    this.apexGlobal = options.apexGlobal;
    this.outbox = new ApexOfflineOutbox();
  }

  static parseIslands(html: string): IslandDescriptor[];
  static parseIslands(attrs: Record<string, string>): IslandDescriptor;
  static parseIslands(input: string | Record<string, string>): IslandDescriptor[] | IslandDescriptor {
    if (typeof input === "string") {
      const results: IslandDescriptor[] = [];
      const tagRegex = /<vertebra-island\b([^>]*)>/gi;
      for (const match of input.matchAll(tagRegex)) {
        const attrText = match[1] ?? "";
        const attrs: Record<string, string> = {};
        const attrRegex = /([a-zA-Z0-9_-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g;
        for (const attrMatch of attrText.matchAll(attrRegex)) {
          const key = attrMatch[1];
          const val = attrMatch[2] ?? attrMatch[3] ?? "";
          if (key) {
            attrs[key] = val;
          }
        }
        results.push(ApexRegion.parseIslandAttributes(attrs));
      }
      return results;
    }
    return ApexRegion.parseIslandAttributes(input);
  }

  private static parseIslandAttributes(attrs: Record<string, string>): IslandDescriptor {
    const componentName = attrs["data-component"] ?? attrs["data-island"] ?? "default";
    let props: Record<string, unknown> = {};
    if (attrs["data-props"]) {
      try {
        const decoded = attrs["data-props"]
          .replace(/&quot;/g, '"')
          .replace(/&#x27;/g, "'")
          .replace(/&amp;/g, "&");
        props = JSON.parse(decoded) as Record<string, unknown>;
      } catch {
        props = {};
      }
    }
    const descriptor: IslandDescriptor = {
      tagName: "vertebra-island",
      componentName,
      props,
    };
    if (attrs["data-region-id"]) {
      descriptor.regionId = attrs["data-region-id"];
    }
    if (attrs["data-live-channel"]) {
      descriptor.liveChannel = attrs["data-live-channel"];
    }
    if (attrs["data-bound-items"]) {
      descriptor.boundItems = attrs["data-bound-items"]
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    }
    return descriptor;
  }

  static dispatchDynamicAction(
    root: EventTarget,
    eventName: string,
    detail: Record<string, unknown>,
    apexGlobal?: ApexGlobalNamespace,
  ): boolean;
  static dispatchDynamicAction(
    eventName: string,
    payload: Record<string, unknown>,
    apexGlobal?: ApexGlobalNamespace,
  ): boolean;
  static dispatchDynamicAction(
    rootOrEventName: EventTarget | string,
    eventNameOrPayload: string | Record<string, unknown>,
    detailOrApexGlobal?: Record<string, unknown> | ApexGlobalNamespace,
    maybeApexGlobal?: ApexGlobalNamespace,
  ): boolean {
    if (typeof rootOrEventName === "string") {
      const eventName = rootOrEventName;
      const payload = (eventNameOrPayload as Record<string, unknown>) ?? {};
      const apexRef =
        (detailOrApexGlobal as ApexGlobalNamespace | undefined) ??
        ((globalThis as Record<string, unknown>).apex as ApexGlobalNamespace | undefined);
      if (apexRef?.event?.trigger) {
        apexRef.event.trigger(globalThis.document ?? globalThis, eventName, payload);
        return true;
      }
      return false;
    }

    const root = rootOrEventName;
    const eventName = eventNameOrPayload as string;
    const detail = (detailOrApexGlobal as Record<string, unknown>) ?? {};
    const apexRef =
      maybeApexGlobal ?? ((globalThis as Record<string, unknown>).apex as ApexGlobalNamespace | undefined);
    if (apexRef?.event?.trigger) {
      apexRef.event.trigger(root, eventName, detail);
      return true;
    }
    if (typeof root?.dispatchEvent === "function" && typeof CustomEvent === "function") {
      return root.dispatchEvent(new CustomEvent(eventName, { detail, bubbles: true }));
    }
    return false;
  }

  readPageItem(itemName: string): string {
    const apexRef =
      this.apexGlobal ?? ((globalThis as Record<string, unknown>).apex as ApexGlobalNamespace | undefined);
    if (apexRef) {
      return apexRef.item(itemName).getValue();
    }
    return this.boundItems.get(itemName) ?? "";
  }

  writePageItem(
    itemName: string,
    value: string,
    suppressChangeEvent = false,
    options?: { recordOffline?: boolean; etag?: string },
  ): void {
    this.boundItems.set(itemName, value);
    if (options?.recordOffline) {
      this.outbox.enqueueItemMutation(this.regionId, itemName, value, { etag: options.etag });
    }
    const apexRef =
      this.apexGlobal ?? ((globalThis as Record<string, unknown>).apex as ApexGlobalNamespace | undefined);
    if (apexRef) {
      apexRef.item(itemName).setValue(value, undefined, suppressChangeEvent);
    }
  }

  triggerRegionEvent(eventName: string, data: Record<string, unknown> = {}): boolean {
    return ApexRegion.dispatchDynamicAction(
      eventName,
      {
        regionId: this.regionId,
        appId: this.appId,
        pageId: this.pageId,
        sessionId: this.sessionId,
        ...data,
      },
      this.apexGlobal,
    );
  }
}
