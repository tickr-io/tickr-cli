import { Fragment } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { ChevronRight } from 'lucide-react';
import { useInstanceSnapshot, useWorkflowDetail } from '@/api/hooks';
import { formatRunHandle, runHandleSource } from '@/lib/runHandle';

export interface Crumb {
  label: string;
  href: string;
}

/**
 * Build the ancestor trail for a pathname across the Workflows section.
 *
 * The chain is Tickr › Workflows › Workflow › Instance › Task, matching the
 * route shape `/workflows/:wid/instances/:iid/tasks/:tid`. The `Tickr` root
 * links to the dashboard and `Workflows` to the list, so even the bare list
 * page carries `Tickr › Workflows`. Returns [] for non-Workflows routes — flat
 * pages (Dashboard, Settings, Events, Health) carry no breadcrumb.
 */
export function buildBreadcrumbs(pathname: string): Crumb[] {
  const parts = pathname.split('/').filter(Boolean);
  if (parts[0] !== 'workflows') return [];

  const [, wid, instancesSeg, iid, tasksSeg, tid] = parts;
  const crumbs: Crumb[] = [
    { label: 'Tickr', href: '/' },
    { label: 'Workflows', href: '/workflows' },
  ];
  if (wid) {
    crumbs.push({ label: wid, href: `/workflows/${wid}` });
    if (instancesSeg === 'instances' && iid) {
      crumbs.push({ label: `Instance ${iid}`, href: `/workflows/${wid}/instances/${iid}` });
      if (tasksSeg === 'tasks' && tid) {
        crumbs.push({ label: `Task ${tid}`, href: `/workflows/${wid}/instances/${iid}/tasks/${tid}` });
      }
    }
  }
  return crumbs;
}

export function Breadcrumb() {
  const { pathname } = useLocation();
  const crumbs = buildBreadcrumbs(pathname);

  // Data-aware relabel post-pass: the workflow segment reads its slug, not the
  // opaque UUID; the instance segment its Run handle (the absolute scheduled
  // timestamp); the task segment its task name. Each shares the page's query
  // cache key, so this fetches only on deep links where the page hasn't already.
  const parts = pathname.split('/').filter(Boolean);
  const workflowId = parts[0] === 'workflows' && parts[1] ? parts[1] : undefined;
  const instanceId =
    parts[0] === 'workflows' && parts[2] === 'instances' && parts[3] ? parts[3] : undefined;
  const taskInstanceId = instanceId && parts[4] === 'tasks' && parts[5] ? parts[5] : undefined;
  const detail = useWorkflowDetail(workflowId);
  const slug = detail.data?.slug || undefined;
  const inst = useInstanceSnapshot(instanceId);
  const handle = formatRunHandle(inst.data ? runHandleSource(inst.data) : null);
  const taskName = taskInstanceId
    ? inst.data?.task_instances.find((ti) => ti.id === taskInstanceId)?.name
    : undefined;

  if (crumbs.length === 0) return null;
  const labelled = crumbs.map((c) => {
    if (workflowId && slug && c.href === `/workflows/${workflowId}`) {
      return { ...c, label: slug };
    }
    if (instanceId && handle && c.href.endsWith(`/instances/${instanceId}`)) {
      return { ...c, label: handle };
    }
    if (taskInstanceId && taskName && c.href.endsWith(`/tasks/${taskInstanceId}`)) {
      return { ...c, label: taskName };
    }
    return c;
  });

  return (
    <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1 text-sm">
      {labelled.map((crumb, i) => {
        const last = i === labelled.length - 1;
        return (
          <Fragment key={crumb.href}>
            {i > 0 && (
              <ChevronRight size={14} className="shrink-0 text-muted-foreground/50" aria-hidden />
            )}
            {last ? (
              <span aria-current="page" className="truncate font-medium text-foreground">
                {crumb.label}
              </span>
            ) : (
              <Link
                to={crumb.href}
                className="truncate text-muted-foreground transition-colors hover:text-foreground"
              >
                {crumb.label}
              </Link>
            )}
          </Fragment>
        );
      })}
    </nav>
  );
}
