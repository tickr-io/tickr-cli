import { AlertCircle } from 'lucide-react';
import { Alert, AlertDescription, AlertTitle } from './ui/alert';
import { Skeleton } from './ui/skeleton';
import { Button } from './ui/button';

export function QueryError({ error, onRetry }: { error: Error; onRetry?: () => void }) {
  return (
    <Alert variant="destructive">
      <AlertCircle size={16} />
      <AlertTitle>Couldn't load</AlertTitle>
      <AlertDescription className="flex items-center justify-between gap-4">
        <span>{error.message}</span>
        {onRetry && (
          <Button size="sm" variant="outline" onClick={onRetry}>
            Retry
          </Button>
        )}
      </AlertDescription>
    </Alert>
  );
}

export function TableLoading({ rows = 6, cols = 4 }: { rows?: number; cols?: number }) {
  return (
    <div className="space-y-2">
      {Array.from({ length: rows }).map((_, r) => (
        <div key={r} className="flex gap-3">
          {Array.from({ length: cols }).map((_, c) => (
            <Skeleton key={c} className="h-9 flex-1" />
          ))}
        </div>
      ))}
    </div>
  );
}

export function EmptyState({ title, description }: { title: string; description?: string }) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-md border border-dashed border-border p-12 text-center">
      <div className="text-base font-medium">{title}</div>
      {description && <div className="text-sm text-muted-foreground">{description}</div>}
    </div>
  );
}
