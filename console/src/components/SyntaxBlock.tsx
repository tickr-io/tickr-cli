import { useMemo } from 'react';
import hljs from 'highlight.js/lib/core';
import nix from 'highlight.js/lib/languages/nix';

// Modular registration — pull in only the Nix grammar (not all of hljs) so the
// bundle stays lean. The kit loads hljs 11 + the same `nix` language from CDN;
// our app self-hosts it. Token colours are themed in index.css (.ncl-src .hljs-*).
hljs.registerLanguage('nix', nix);

/**
 * Renders Nickel source with Nix syntax highlighting (the DSL is Nix-flavoured;
 * the kit highlights it with hljs's `nix` grammar). `hljs.highlight` escapes the
 * input, so the produced markup is safe to inject.
 */
export function SyntaxBlock({ code }: { code: string }) {
  const html = useMemo(() => hljs.highlight(code, { language: 'nix' }).value, [code]);
  return (
    <pre className="pre ncl-src">
      <code className="hljs language-nix" dangerouslySetInnerHTML={{ __html: html }} />
    </pre>
  );
}
