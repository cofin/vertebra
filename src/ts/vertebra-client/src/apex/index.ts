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

export function dispatchApexDynamicAction(
  eventName: string,
  payload: Record<string, unknown>,
  apexGlobal?: ApexGlobalNamespace,
): boolean {
  const apexRef = apexGlobal ?? ((globalThis as Record<string, unknown>).apex as ApexGlobalNamespace | undefined);
  if (apexRef?.event?.trigger) {
    apexRef.event.trigger(globalThis.document ?? globalThis, eventName, payload);
    return true;
  }
  return false;
}

export class VertebraApexRegionBridge {
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
    return dispatchApexDynamicAction(
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

export interface VertebraIslandDescriptor {
  tagName: string;
  componentName: string;
  props: Record<string, unknown>;
}

export function parseVertebraIslandAttributes(attrs: Record<string, string>): VertebraIslandDescriptor {
  const componentName = attrs["data-component"] ?? "default";
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
