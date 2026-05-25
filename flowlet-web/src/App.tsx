import { createBrowserRouter, RouterProvider, Link, useLocation, Outlet } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Breadcrumb } from '@/components/Breadcrumb';
import Dashboard from '@/pages/Dashboard';
import FlowsList from '@/pages/FlowsList';
import FlowRun from '@/pages/FlowRun';
import RunsList from '@/pages/RunsList';
import RunDetail from '@/pages/RunDetail';

const queryClient = new QueryClient();

function useBreadcrumbs() {
  const loc = useLocation();
  const segs = loc.pathname.split('/').filter(Boolean);
  const crumbs = [{ label: 'Home', url: '/' }];
  if (segs[0] === 'flows') {
    crumbs.push({ label: 'Flows', url: '/flows' });
    if (segs[1] && segs[2] === 'run') crumbs.push({ label: `Run: ${segs[1]}`, url: loc.pathname });
  } else if (segs[0] === 'runs') {
    crumbs.push({ label: 'Runs', url: '/runs' });
    if (segs[1]) crumbs.push({ label: segs[1].slice(0, 8) + '...', url: loc.pathname });
  }
  return crumbs;
}

function Layout() {
  const crumbs = useBreadcrumbs();
  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-indigo-700 text-white px-6 py-3 flex items-center gap-6">
        <Link to="/" className="font-bold text-lg"><strong>Flowlet</strong> – Workflow Orchestration</Link>
        <div className="flex-1" />
        {([['/', 'Dashboard'], ['/flows', 'Flows'], ['/runs', 'Runs']] as [string, string][]).map(([path, label]) => (
          <Link key={path} to={path} className="hover:text-indigo-200 text-sm font-medium">{label}</Link>
        ))}
      </nav>
      <Breadcrumb items={crumbs} />
      <Outlet />
    </div>
  );
}

const router = createBrowserRouter([
  {
    path: '/',
    element: <Layout />,
    children: [
      { index: true, element: <Dashboard /> },
      { path: 'flows', element: <FlowsList /> },
      { path: 'flows/:name/run', element: <FlowRun /> },
      { path: 'runs', element: <RunsList /> },
      { path: 'runs/:id', element: <RunDetail /> },
    ],
  },
]);

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}
