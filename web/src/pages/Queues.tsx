import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { shortEid, timeAgo } from '@/lib/format';

/** Depth is read from the queue, not from a projection: the control log
 *  records an enqueue and not an ack, so only the queue knows what is on it
 *  now (spec 00, principle 7). */
export default function Queues() {
  const [selected, setSelected] = useState<string | null>(null);
  const queues = useQuery({ queryKey: ['queues'], queryFn: api.queues, refetchInterval: 5_000 });
  const tasks = useQuery({
    queryKey: ['tasks', selected],
    queryFn: () => api.tasks(selected!),
    enabled: !!selected,
    refetchInterval: 5_000,
  });

  return (
    <div className="space-y-4 p-6">
      <h1 className="text-2xl font-semibold text-slate-900">Queues</h1>

      <div className="overflow-x-auto rounded border border-slate-200 bg-white">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-left text-xs uppercase text-slate-500">
            <tr>{['Queue', 'Total', 'Visible', 'Claimed', ''].map(h =>
              <th key={h} className="px-3 py-2 font-medium">{h}</th>)}</tr>
          </thead>
          <tbody>
            {queues.data?.items.map(q => (
              <tr key={q.queue} className="border-t border-slate-100">
                <td className="px-3 py-2 font-medium">{q.queue}</td>
                <td className="px-3 py-2">{q.total}</td>
                <td className="px-3 py-2">{q.visible}</td>
                <td className="px-3 py-2 text-slate-500" title="An upper bound: a dead holder's lease document stays until a steal or an ack.">
                  ≤ {q.claimed}
                </td>
                <td className="px-3 py-2">
                  <button
                    onClick={() => setSelected(s => (s === q.queue ? null : q.queue))}
                    className="text-indigo-600 hover:underline"
                  >{selected === q.queue ? 'hide' : 'tasks'}</button>
                </td>
              </tr>
            ))}
            {!queues.data?.items.length && (
              <tr><td colSpan={5} className="px-3 py-6 text-center text-slate-400">no queue configured</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {selected && (
        <div className="overflow-x-auto rounded border border-slate-200 bg-white">
          <h2 className="border-b border-slate-200 px-3 py-2 font-medium">{selected}</h2>
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase text-slate-500">
              <tr>{['Task', 'Kind', 'Execution', 'Reason', 'Enqueued', 'By'].map(h =>
                <th key={h} className="px-3 py-2 font-medium">{h}</th>)}</tr>
            </thead>
            <tbody>
              {tasks.data?.items.map(task => (
                <tr key={task.task_id} className="border-t border-slate-100">
                  <td className="px-3 py-1.5 font-mono text-xs text-slate-600">{task.task_id}</td>
                  <td className="px-3 py-1.5">{task.kind}</td>
                  <td className="px-3 py-1.5">
                    <Link to={`/executions/${task.eid}`} className="font-mono text-xs text-indigo-600">
                      {shortEid(task.eid)}
                    </Link>
                  </td>
                  <td className="px-3 py-1.5 text-slate-500">{task.reason}</td>
                  <td className="px-3 py-1.5 text-slate-500">{timeAgo(task.enqueued_at)}</td>
                  <td className="px-3 py-1.5 text-slate-500">{task.enqueued_by.id}</td>
                </tr>
              ))}
              {!tasks.data?.items.length && (
                <tr><td colSpan={6} className="px-3 py-6 text-center text-slate-400">the queue is empty</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
