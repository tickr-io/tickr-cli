import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useTheme } from '@/contexts/ThemeContext';
import { useTenant } from '@/api/hooks';
import { Sun, Moon, Monitor } from 'lucide-react';

function TenantCard() {
  const { data, isLoading, isError } = useTenant();
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Tenant</CardTitle>
        <CardDescription>The tenant this Tickr instance serves.</CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <p className="text-sm text-muted-foreground">Loading…</p>
        ) : isError || !data ? (
          <p className="text-sm text-muted-foreground">Tenant info unavailable.</p>
        ) : (
          <dl className="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
            <dt className="text-muted-foreground">Slug</dt>
            <dd className="font-medium">{data.slug}</dd>
            <dt className="text-muted-foreground">ID</dt>
            <dd className="font-mono text-xs break-all">{data.id}</dd>
            <dt className="text-muted-foreground">Workflows</dt>
            <dd className="font-medium tabular-nums">{data.workflow_count}</dd>
          </dl>
        )}
      </CardContent>
    </Card>
  );
}

export function SettingsPage() {
  const { theme, setTheme } = useTheme();
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
        <p className="text-sm text-muted-foreground">App preferences.</p>
      </div>
      <TenantCard />
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Appearance</CardTitle>
          <CardDescription>Choose how Tickr looks.</CardDescription>
        </CardHeader>
        <CardContent className="flex gap-2">
          <Button variant={theme === 'light' ? 'default' : 'outline'} onClick={() => setTheme('light')}>
            <Sun size={16} className="mr-2" />
            Light
          </Button>
          <Button variant={theme === 'dark' ? 'default' : 'outline'} onClick={() => setTheme('dark')}>
            <Moon size={16} className="mr-2" />
            Dark
          </Button>
          <Button variant={theme === 'system' ? 'default' : 'outline'} onClick={() => setTheme('system')}>
            <Monitor size={16} className="mr-2" />
            System
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
