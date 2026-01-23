import { useEffect, useState } from "react";
import type { Subscription } from "./index";

export function useVertebraState<T>(subscription: Subscription<T>): T | undefined {
  const [state, setState] = useState<T>();

  useEffect(() => {
    const unsubscribe = subscription.subscribe((data) => {
      setState(data);
    });
    return () => {
      unsubscribe();
    };
  }, [subscription]);

  return state;
}
