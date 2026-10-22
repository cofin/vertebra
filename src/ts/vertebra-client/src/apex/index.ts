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
  props: Record<string, unknown>;
}

export class ApexRegion {
  readonly regionId: string;
  readonly appId: number;
  readonly pageId: number;
  readonly sessionId: number;
  private readonly apexGlobal?: ApexGlobalNamespace;
  private readonly boundItems = new Map<string, string>();

  constructor(options: ApexRegionBridgeOptions) {
    this.regionId = options.regionId;
    this.appId = options.appId;
    this.pageId = options.pageId;
    this.sessionId = options.sessionId;
    this.apexGlobal = options.apexGlobal;
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
        props = JSON.parse(attrs["data-props"]) as Record<string, unknown>;
      } catch {
        props = {};
      }
    }
    return {
      tagName: "vertebra-island",
      componentName,
      props,
    };
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

  writePageItem(itemName: string, value: string, suppressChangeEvent = false): void {
    this.boundItems.set(itemName, value);
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
