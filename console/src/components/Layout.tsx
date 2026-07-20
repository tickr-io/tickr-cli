import { useState } from 'react';
import { Link, NavLink, Outlet } from 'react-router-dom';
import {
  LayoutDashboard,
  Workflow,
  ScrollText,
  HeartPulse,
  Settings,
  PanelLeftClose,
  PanelLeftOpen,
  Sun,
  Moon,
  Monitor,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { Logo } from './Logo';
import { Breadcrumb } from './Breadcrumb';
import { Separator } from './ui/separator';
import { Button } from './ui/button';
import { useTheme } from '@/contexts/ThemeContext';

interface NavItem {
  to: string;
  label: string;
  icon: React.ComponentType<{ className?: string; size?: number }>;
  disabled?: boolean;
}

interface NavGroup {
  label: string;
  items: NavItem[];
}

// Nav grouping principle: Overview holds primary tenant surfaces; System holds
// orchestration & ops internals. Event log lives in System because it surfaces
// system/orchestration events (task transitions, state changes), not a tenant
// activity feed. Health is an ops surface and belongs alongside it. Event log
// and Health are navigable today but lead to honest NeedsBackend pages. There is
// deliberately no global cross-workflow instances
// entry — a workflow's instances are scoped to that workflow (DC-0003).
const navGroups: NavGroup[] = [
  {
    label: 'Overview',
    items: [
      { to: '/', label: 'Dashboard', icon: LayoutDashboard },
      { to: '/workflows', label: 'Workflows', icon: Workflow },
    ],
  },
  {
    label: 'System',
    items: [
      { to: '/events', label: 'Event log', icon: ScrollText },
      { to: '/health', label: 'Health', icon: HeartPulse },
      { to: '/settings', label: 'Settings', icon: Settings },
    ],
  },
];

function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const next = theme === 'light' ? 'dark' : theme === 'dark' ? 'system' : 'light';
  const Icon = theme === 'light' ? Sun : theme === 'dark' ? Moon : Monitor;
  return (
    <Button
      variant="ghost"
      size="icon"
      onClick={() => setTheme(next)}
      title={`Theme: ${theme}`}
      aria-label={`Theme: ${theme}`}
    >
      <Icon size={18} />
    </Button>
  );
}

export function Layout() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background text-foreground">
      <aside
        className={cn(
          'flex h-full flex-col border-r border-border bg-card transition-[width] duration-200',
          collapsed ? 'w-[var(--sidebar-width-collapsed)]' : 'w-[var(--sidebar-width)]',
        )}
      >
        <Link
          to="/"
          aria-label="Tickr — go to Dashboard"
          className="flex h-16 items-center gap-3 px-4 transition-opacity hover:opacity-80"
        >
          <Logo size={28} />
          {!collapsed && (
            <span className="text-xl font-semibold tracking-tight">Tickr</span>
          )}
        </Link>
        <Separator />
        <div className="sidebar-scroll flex-1 overflow-y-auto py-3">
          {navGroups.map((group) => (
            <div key={group.label} className="mb-4">
              {!collapsed && (
                <div className="px-4 pb-1 text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                  {group.label}
                </div>
              )}
              <ul className="space-y-0.5 px-2">
                {group.items.map((item) => {
                  const Icon = item.icon;
                  const inner = (
                    <>
                      <Icon size={18} className="shrink-0" />
                      {!collapsed && (
                        <span className="flex-1 truncate text-left text-sm">{item.label}</span>
                      )}
                    </>
                  );
                  if (item.disabled) {
                    return (
                      <li key={item.to}>
                        <div
                          className="flex cursor-not-allowed items-center gap-3 rounded-md px-3 py-2 text-muted-foreground/60"
                          title={`${item.label} — needs backend support`}
                        >
                          {inner}
                        </div>
                      </li>
                    );
                  }
                  return (
                    <li key={item.to}>
                      <NavLink
                        to={item.to}
                        end={item.to === '/'}
                        className={({ isActive }) =>
                          cn(
                            'flex items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors',
                            isActive
                              ? 'bg-primary/10 text-primary'
                              : 'text-foreground/80 hover:bg-accent hover:text-foreground',
                          )
                        }
                      >
                        {inner}
                      </NavLink>
                    </li>
                  );
                })}
              </ul>
            </div>
          ))}
        </div>
        <Separator />
        <div className="flex items-center justify-between p-2">
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setCollapsed((v) => !v)}
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {collapsed ? <PanelLeftOpen size={18} /> : <PanelLeftClose size={18} />}
          </Button>
          {!collapsed && <ThemeToggle />}
        </div>
      </aside>

      <main className="flex h-full flex-1 flex-col overflow-hidden">
        <header className="flex h-16 items-center justify-between border-b border-border bg-background/60 px-6 backdrop-blur">
          <Breadcrumb />
          {collapsed && <ThemeToggle />}
        </header>
        <div className="flex-1 overflow-y-auto p-6">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
