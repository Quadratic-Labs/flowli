import {
  createBrowserRouter, Link, Outlet, RouterProvider, useLocation, useRouteError,
} from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Breadcrumb, type Crumb } from '@/components/Breadcrumb';
import { TokenBar } from '@/components/TokenBar';
import { ApiError } from '@/lib/api';
import Dashboard from '@/pages/Dashboard';
import ExecutionsList from '@/pages/ExecutionsList';
import ExecutionDetail from '@/pages/ExecutionDetail';
import WorkflowsList from '@/pages/WorkflowsList';
import StartExecution from '@/pages/StartExecution';
import Reviews from '@/pages/Reviews';
import Queues from '@/pages/Queues';

const client = new QueryClient({
  defaultOptions: {
    queries: {
      // The ETag cache hands back the same object on a 304, so a poll that
      // changes nothing costs one request and no render.
      staleTime: 1_000,
      retry: (count, error) =>
        !(error instanceof ApiError && error.status < 500) && count < 2,
    },
  },
});

const NAV: [string, string][] = [
  ['/', 'Dashboard'],
  ['/executions', 'Executions'],
  ['/workflows', 'Workflows'],
  ['/reviews', 'Reviews'],
  ['/queues', 'Queues'],
];

function useCrumbs(): Crumb[] {
  const { pathname } = useLocation();
  const segments = pathname.split('/').filter(Boolean);
  const crumbs: Crumb[] = [{ label: 'Home', url: '/' }];
  if (segments[0] === 'executions') {
    crumbs.push({ label: 'Executions', url: '/executions' });
    if (segments[1]) crumbs.push({ label: `${segments[1].slice(0, 8)}…`, url: pathname });
  } else if (segments[0] === 'workflows') {
    crumbs.push({ label: 'Workflows', url: '/workflows' });
    if (segments[1]) {
      crumbs.push({ label: decodeURIComponent(segments[1]), url: pathname });
    }
  } else if (segments[0]) {
    crumbs.push({ label: segments[0][0].toUpperCase() + segments[0].slice(1), url: pathname });
  }
  return crumbs;
}

function Layout() {
  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <nav className="flex items-center gap-6 bg-indigo-700 px-6 py-3 text-white">
        <Link to="/" className="text-lg font-bold">flowli</Link>
        {NAV.map(([path, label]) => (
          <Link key={path} to={path} className="text-sm font-medium hover:text-indigo-200">{label}</Link>
        ))}
        <div className="grow" />
        <TokenBar />
      </nav>
      <Breadcrumb items={useCrumbs()} />
      <Outlet />
    </div>
  );
}

/** One place to say what a refusal means: the service answers RFC 9457, and a
 *  401 or a 403 is about the token, not about the app being broken. */
function ErrorScreen() {
  const error = useRouteError();
  const problem = error instanceof ApiError ? error : null;
  return (
    <div className="p-10">
      <h1 className="text-xl font-semibold text-slate-900">
        {problem?.status === 401 ? 'Not signed in'
          : problem?.status === 403 ? 'Not permitted'
          : 'Something went wrong'}
      </h1>
      <p className="mt-2 text-slate-600">
        {problem?.status === 401 ? 'Set an access token in the bar above.'
          : problem?.message ?? String(error)}
      </p>
      <Link to="/" className="mt-4 inline-block text-indigo-600 hover:underline">back to the dashboard</Link>
    </div>
  );
}

const router = createBrowserRouter([
  {
    path: '/',
    element: <Layout />,
    errorElement: <ErrorScreen />,
    children: [
      { index: true, element: <Dashboard /> },
      { path: 'executions', element: <ExecutionsList /> },
      { path: 'executions/:eid', element: <ExecutionDetail /> },
      { path: 'workflows', element: <WorkflowsList /> },
      { path: 'workflows/:name/:version/start', element: <StartExecution /> },
      { path: 'reviews', element: <Reviews /> },
      { path: 'queues', element: <Queues /> },
    ],
  },
]);

export default function App() {
  return (
    <QueryClientProvider client={client}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}
