import { useEffect, useState } from 'react';

/**
 * 1Hz wall-clock tick for analog hands and past/future shading.
 * Pauses while the document is hidden so we don't burn CPU in a background tab.
 */
export function useNow(intervalMs = 1000): Date {
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    let id: number | null = null;
    const start = () => {
      if (id !== null) return;
      id = window.setInterval(() => setNow(new Date()), intervalMs);
    };
    const stop = () => {
      if (id !== null) {
        window.clearInterval(id);
        id = null;
      }
    };
    const onVis = () => {
      if (document.visibilityState === 'visible') {
        setNow(new Date());
        start();
      } else {
        stop();
      }
    };

    if (document.visibilityState === 'visible') start();
    document.addEventListener('visibilitychange', onVis);
    return () => {
      document.removeEventListener('visibilitychange', onVis);
      stop();
    };
  }, [intervalMs]);

  return now;
}
