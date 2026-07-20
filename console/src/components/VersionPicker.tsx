import { useState } from 'react';
import { ChevronDown, Check } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import type { AvailableVersion } from '@/api/client';

/** Dot color for a raw `workflows.status`, sharing the DC-0001 tokens. */
function statusDot(status: string): string {
  switch (status) {
    case 'Ready':
    case 'Submitted':
      return 'bg-success';
    case 'Building':
      return 'bg-info';
    case 'BuildFailed':
      return 'bg-destructive';
    default:
      return 'bg-muted-foreground';
  }
}

function fmtRegistered(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

/**
 * The Version cell rendered as an inline picker (DC-0010). Lists every
 * registered version with its build-status dot and registration time, ordered
 * as the server returns them (newest first). Selecting a version invokes
 * `onChange`, which the page reflects into the URL's `?version=X` and refetches.
 */
export function VersionPicker({
  currentVersion,
  availableVersions,
  onChange,
}: {
  currentVersion: number;
  availableVersions: AvailableVersion[];
  onChange: (version: number) => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        className="inline-flex items-center gap-1.5 rounded-md font-mono text-sm hover:text-primary"
        aria-label="select version"
      >
        v{currentVersion}
        <ChevronDown size={14} className="text-muted-foreground" />
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 p-1">
        <ul role="listbox" className="max-h-72 overflow-auto">
          {availableVersions.map((v) => {
            const selected = v.version === currentVersion;
            return (
              <li key={v.version}>
                <button
                  type="button"
                  role="option"
                  aria-selected={selected}
                  onClick={() => {
                    onChange(v.version);
                    setOpen(false);
                  }}
                  className="flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-left text-sm hover:bg-accent"
                >
                  <Check
                    size={14}
                    className={selected ? 'opacity-100' : 'opacity-0'}
                    aria-hidden="true"
                  />
                  <span className="font-mono">v{v.version}</span>
                  <span
                    className={`ml-auto inline-flex items-center gap-1.5 text-xs text-muted-foreground`}
                  >
                    <span
                      className={`inline-block size-2 rounded-full ${statusDot(v.status)}`}
                      aria-hidden="true"
                    />
                    {v.status}
                  </span>
                </button>
                <div className="px-2 pb-1 pl-8 text-xs text-muted-foreground">
                  {fmtRegistered(v.inserted_at)}
                </div>
              </li>
            );
          })}
        </ul>
      </PopoverContent>
    </Popover>
  );
}
