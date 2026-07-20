import { Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { DashboardPage } from './pages/DashboardPage';
import { WorkflowsPage } from './pages/WorkflowsPage';
import { WorkflowDetailPage } from './pages/WorkflowDetailPage';
import { InstanceDetailPage } from './pages/InstanceDetailPage';
import { TaskDetailPage } from './pages/TaskDetailPage';
import { EventsPage } from './pages/EventsPage';
import { HealthPage } from './pages/HealthPage';
import { SettingsPage } from './pages/SettingsPage';
import { NotFoundPage } from './pages/NotFoundPage';

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<DashboardPage />} />
        <Route path="workflows" element={<WorkflowsPage />} />
        <Route path="workflows/:workflowId" element={<WorkflowDetailPage />} />
        <Route path="workflows/:workflowId/instances/:instanceId" element={<InstanceDetailPage />} />
        <Route
          path="workflows/:workflowId/instances/:instanceId/tasks/:taskId"
          element={<TaskDetailPage />}
        />
        <Route path="events" element={<EventsPage />} />
        <Route path="health" element={<HealthPage />} />
        <Route path="settings" element={<SettingsPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  );
}
