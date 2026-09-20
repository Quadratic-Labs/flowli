import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { ReviewPanel } from '@/components/ReviewPanel';
import { shortEid, timeAgo } from '@/lib/format';

/** The inbox: reviews waiting for a person. The queue of each row is what
 *  gates the decision, and the service checks it — a token without
 *  `reviews:decide:{queue}` gets a 403 when it tries. */
export default function Reviews() {
  const [queue, setQueue] = useState('');
  const { data, isLoading } = useQuery({
    queryKey: ['reviews', queue],
    queryFn: () => api.reviews(queue || undefined),
    refetchInterval: 10_000,
  });

  const queues = [...new Set((data?.items ?? []).map(r => r.queue))];

  return (
    <div className="space-y-4 p-6">
      <div className="flex items-center gap-3">
        <h1 className="text-2xl font-semibold text-slate-900">Reviews</h1>
        <select
          value={queue}
          onChange={e => setQueue(e.target.value)}
          className="rounded border border-slate-300 px-2 py-1 text-sm"
        >
          <option value="">every queue</option>
          {queues.map(q => <option key={q} value={q}>{q}</option>)}
        </select>
      </div>

      {isLoading && <p className="text-slate-400">loading…</p>}
      {!isLoading && !data?.items.length && (
        <p className="text-slate-400">Nothing is waiting for a decision.</p>
      )}

      <div className="grid gap-4 lg:grid-cols-2">
        {data?.items.map(review => (
          <div key={review.rid} className="space-y-2">
            <div className="flex items-center gap-2 text-sm text-slate-500">
              <span>asked {timeAgo(review.requested_at)}</span>
              {review.eid && (
                <Link to={`/executions/${review.eid}`} className="font-mono text-xs text-indigo-600">
                  {shortEid(review.eid)}
                </Link>
              )}
            </div>
            <ReviewPanel review={review} />
          </div>
        ))}
      </div>
    </div>
  );
}
