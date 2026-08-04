/**
 * Tickr clock logo: an open day-progress ring around two hands and a central
 * pivot. Teal via `--primary`, so the mark themes in light and dark.
 */
export function Logo({ size = 28, className }: { size?: number; className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 32 32"
      fill="none"
      className={className}
      aria-hidden
    >
      <g
        stroke="hsl(var(--primary))"
        strokeWidth="2.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        <path d="M22 5.6 A 12 12 0 1 1 10 5.6" />
        <path d="M16 16 V2.4" />
        <path d="M16 16 L21.6 12.6" />
      </g>
      <circle cx="16" cy="16" r="1.6" fill="hsl(var(--primary))" />
    </svg>
  );
}
