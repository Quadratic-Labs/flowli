import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Ban } from 'lucide-react';
import { api } from '@/lib/api';

/** Request cancellation of an active run.
 *
 *  Pending runs are closed as canceled immediately; running runs are flagged
 *  and stop at their next heartbeat, so the status may stay "running" for a
 *  short while after a successful request. */
export function CancelRunButton({ runId, small = false, onDone }: {
  runId: string;
  small?: boolean;
  onDone?: (status: string) => void;
}) {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: () => api.cancelRun(runId),
    onSuccess: (resp) => {
      queryClient.invalidateQueries({ queryKey: ['runs'] });
      queryClient.invalidateQueries({ queryKey: ['run', runId] });
      queryClient.invalidateQueries({ queryKey: ['run-events', runId] });
      onDone?.(resp.status);
    },
  });

  return (
    <span className="inline-flex items-center gap-2">
      <button
        onClick={() => { if (window.confirm('Cancel this run?')) mutation.mutate(); }}
        disabled={mutation.isPending}
        title="Request cancellation — running flows stop at their next heartbeat"
        className={`inline-flex items-center gap-1.5 rounded text-white bg-red-600 hover:bg-red-700 disabled:opacity-50 ${
          small ? 'px-3 py-1.5 text-sm' : 'px-4 py-2'
        }`}
      >
        {mutation.isPending
          ? <div className="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />
          : <Ban size={small ? 14 : 16} />}
        Cancel
      </button>
      {mutation.isError && (
        <span className="text-red-600 text-xs">{String(mutation.error)}</span>
      )}
      {mutation.isSuccess && mutation.data.status === 'running' && (
        <span className="text-amber-600 text-xs">cancel requested — stops at next heartbeat</span>
      )}
    </span>
  );
}
