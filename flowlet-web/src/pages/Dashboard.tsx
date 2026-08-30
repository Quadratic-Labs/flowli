import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { RefreshCw, CheckCircle, AlertTriangle, XCircle, HelpCircle } from 'lucide-react';
import { getDashboardMetrics, timeAgo } from '@/lib/api';
import { StatusBadge } from '@/components/StatusBadge';

export default function Dashboard() {
  const nav = useNavigate();
  const { data: metrics, isLoading, error, refetch } = useQuery({
    queryKey: ['dashboard'],
    queryFn: getDashboardMetrics,
  });

  const healthIcon = () => {
    if (!metrics) return <HelpCircle size={72} color="#9e9e9e" />;
    if (metrics.healthStatus === 'green') return <CheckCircle size={72} color="#4caf50" />;
    if (metrics.healthStatus === 'orange') return <AlertTriangle size={72} color="#ff9800" />;
    return <XCircle size={72} color="#f44336" />;
  };
  const healthColor = !metrics ? '#9e9e9e' : metrics.healthStatus === 'green' ? '#4caf50' : metrics.healthStatus === 'orange' ? '#ff9800' : '#f44336';
  const healthText = !metrics ? 'Unknown' : metrics.healthStatus === 'green' ? 'Healthy' : metrics.healthStatus === 'orange' ? 'Warning' : 'Degraded';

  const allFailures = metrics ? [...metrics.failedRuns, ...metrics.longRunningFlows] : [];

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-normal">Operator Dashboard</h1>
        <button onClick={() => refetch()} disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50">
          <RefreshCw size={16} className={isLoading ? 'animate-spin' : ''} /> Refresh
        </button>
      </div>

      {isLoading && <div className="flex justify-center p-12"><div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" /></div>}
      {error && <div className="text-red-600 bg-red-50 p-4 rounded mb-4">{String(error)}</div>}

      {metrics && !isLoading && (
        <div className="flex flex-col gap-6">
          {/* Health card */}
          <div className="bg-white rounded shadow border-l-4 p-6" style={{ borderLeftColor: '#1976d2' }}>
            <h2 className="text-lg font-medium mb-4">Health Status</h2>
            <div className="flex items-center gap-6">
              {healthIcon()}
              <div>
                <div className="text-3xl font-medium" style={{ color: healthColor }}>{healthText}</div>
                <div className="text-sm text-gray-500">Last updated: {timeAgo(metrics.lastUpdated.toISOString())}</div>
              </div>
            </div>
          </div>

          {/* Summary cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            {[
              { label: 'Total Flows (24h)', value: metrics.totalFlows, color: 'text-gray-800' },
              { label: `Successful (${metrics.successRate.toFixed(1)}%)`, value: metrics.successfulFlows, color: 'text-green-600' },
              { label: `Failed (${metrics.failureRate.toFixed(1)}%)`, value: metrics.failedFlows, color: 'text-red-600' },
              { label: 'Currently Running', value: metrics.runningFlows, color: 'text-orange-500' },
              { label: 'Needs Review', value: metrics.gatedRuns.length, color: 'text-purple-600' },
            ].map(card => (
              <div key={card.label} className="bg-white rounded shadow p-6 text-center">
                <div className={`text-5xl font-medium mb-2 ${card.color}`}>{card.value}</div>
                <div className="text-sm text-gray-500">{card.label}</div>
              </div>
            ))}
          </div>

          {/* Gated runs — need a human review, not a failure */}
          {metrics.gatedRuns.length > 0 && (
            <div className="bg-white rounded shadow p-6 border-l-4" style={{ borderLeftColor: '#9c27b0' }}>
              <h2 className="text-lg font-medium mb-4">Needs Review</h2>
              <table className="w-full text-sm">
                <thead><tr className="text-left border-b">
                  <th className="py-2 pr-4">Flow Name</th><th className="py-2 pr-4">Status</th>
                  <th className="py-2 pr-4">Started</th><th className="py-2">Actions</th>
                </tr></thead>
                <tbody>{metrics.gatedRuns.map(run => (
                  <tr key={run.run_id} className="border-b hover:bg-gray-50">
                    <td className="py-2 pr-4 font-medium">{run.flow_name}</td>
                    <td className="py-2 pr-4"><StatusBadge status={run.status} /></td>
                    <td className="py-2 pr-4">{timeAgo(run.started_at)}</td>
                    <td className="py-2">
                      <button onClick={() => nav(`/runs/${run.run_id}`)}
                        className="text-blue-600 hover:underline">Review</button>
                    </td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          )}

          {/* Failures table */}
          <div className="bg-white rounded shadow p-6">
            <h2 className="text-lg font-medium mb-4">Failures &amp; Alerts</h2>
            {allFailures.length === 0 ? (
              <div className="flex items-center gap-3 text-green-600 p-6 justify-center">
                <CheckCircle size={28} /> No failures or long-running flows in the last 24 hours
              </div>
            ) : (
              <table className="w-full text-sm">
                <thead><tr className="text-left border-b">
                  <th className="py-2 pr-4">Flow Name</th><th className="py-2 pr-4">Status</th>
                  <th className="py-2 pr-4">Duration</th><th className="py-2 pr-4">Started</th>
                  <th className="py-2">Actions</th>
                </tr></thead>
                <tbody>{allFailures.map(run => (
                  <tr key={run.run_id} className="border-b hover:bg-gray-50">
                    <td className="py-2 pr-4 font-medium">{run.flow_name}</td>
                    <td className="py-2 pr-4"><StatusBadge status={run.status} /></td>
                    <td className="py-2 pr-4">—</td>
                    <td className="py-2 pr-4">{timeAgo(run.started_at)}</td>
                    <td className="py-2">
                      <button onClick={() => nav(`/runs/${run.run_id}`)}
                        className="text-blue-600 hover:underline">View Details</button>
                    </td>
                  </tr>
                ))}</tbody>
              </table>
            )}
          </div>

          {/* Performance */}
          <div className="bg-gray-50 rounded shadow p-6">
            <h2 className="text-lg font-medium mb-4">Performance (Last 24h)</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {[
                { label: 'Total Compute Time', value: metrics.totalComputeTime },
                { label: 'Average Execution Time', value: metrics.averageExecutionTime },
                { label: 'Success Rate', value: `${metrics.successRate.toFixed(1)}%` },
              ].map(item => (
                <div key={item.label} className="text-center">
                  <div className="text-sm text-gray-500 mb-2">{item.label}</div>
                  <div className="text-2xl font-medium text-blue-700">{item.value}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
