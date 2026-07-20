// Registers jest-dom matchers (toBeInTheDocument, etc.) on Vitest's expect and
// augments its types. Imported via the Vitest setupFiles hook.
import '@testing-library/jest-dom/vitest';

// jsdom has no ResizeObserver; the dashboard clock measures its host with it.
if (typeof globalThis.ResizeObserver === 'undefined') {
  globalThis.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  };
}

// jsdom has no matchMedia; ThemeProvider reads the system color-scheme through
// it. Provide a minimal, no-op stub so components mount under test.

// Radix popovers/dropdowns call these DOM APIs jsdom doesn't implement. Stub
// them so an opened Version picker (and any future overlay) mounts under test.
if (typeof Element !== 'undefined') {
  if (!Element.prototype.hasPointerCapture) {
    Element.prototype.hasPointerCapture = () => false;
  }
  if (!Element.prototype.releasePointerCapture) {
    Element.prototype.releasePointerCapture = () => {};
  }
  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = () => {};
  }
}

if (typeof window !== 'undefined' && !window.matchMedia) {
  window.matchMedia = (query: string): MediaQueryList =>
    ({
      matches: false,
      media: query,
      onchange: null,
      addEventListener: () => {},
      removeEventListener: () => {},
      addListener: () => {},
      removeListener: () => {},
      dispatchEvent: () => false,
    }) as unknown as MediaQueryList;
}
