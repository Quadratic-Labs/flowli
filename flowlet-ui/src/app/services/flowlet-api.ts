import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

// New API models matching backend DTOs
export interface RunSummaryDTO {
  span_id: string;
  span_name: string;
  status: string;
  start_ts: string;
  end_ts: string;
  duration: string | null;
  children: RunSummaryDTO[];
}

export interface SpanLogDTO {
  flow_name: string | null;
  run_id: string | null;
  span_type: string | null;
  span_name: string | null;
  span_id: string | null;
  parent_span_id: string | null;
  ts: string | null;
  message: string | null;
  level: string | null;
}

export interface RunDTO extends RunSummaryDTO {
  logs: SpanLogDTO[];
}



export interface StartFlowRequest {
  kwargs?: { [key: string]: any };
}

export interface FlowSubmissionResponse {
  job_id: string;
  submitted_at: string;
}

// Flow schema from /flows endpoint
export interface FlowSchema {
  name: string;
  docstring?: string | null;
  has_schema: boolean;
  parameters?: Array<{
    name: string;
    type: string;
    required: boolean;
    default?: any;
  }>;
}

// RunStateDTO — matches POST /runs/query response (worker execution state from SQLite snapshot)
export interface RunStateDTO {
  run_id: string;
  flow_name: string;
  status: string;
  worker_id: string;
  started_at: string;
  heartbeat_at: string;
  ended_at: string | null;
  attempt: number;
  max_retries: number;
}

// Legacy interfaces for backward compatibility (to be removed)
export interface FlowListItem {
  name: string;
  status: string | null;
  started_at: string | null;
  last_at: string | null;
  finished_ago: string | null;
  duration: string | null;
  doc?: string | null;
}

export interface FlowRunInfo {
  name: string;
  run_id: string;
  status: string | null;
  ended_at: string | null;
}

export interface RunAttrInfo {
  name: string;
  run_type: string;
  run_id: string;
}

export interface RunLogInfo {
  status: string;
  log: string;
  timestamp: string;
  log_id: string;
}

export interface RunDetailInfo {
  run: RunAttrInfo;
  logs: RunLogInfo[];
  parent: RunAttrInfo | null;
  children: RunDetailInfo[];
}

@Injectable({
  providedIn: 'root'
})
export class FlowletApi {
  // Use proxy in development (/api) or direct URL in production
  private apiUrl = '/api';

  constructor(private http: HttpClient) {}

  // Get list of flows (using introspection endpoint)
  getFlows(): Observable<FlowSchema[]> {
    return this.http.get<FlowSchema[]>(`${this.apiUrl}/flows`);
  }

  // Run a flow synchronously
  runFlow(flowName: string, payload: StartFlowRequest = {}): Observable<void> {
    return this.http.post<void>(`${this.apiUrl}/execute/${flowName}`, payload);
  }

  // Submit a flow for asynchronous execution
  submitFlow(flowName: string, payload: StartFlowRequest = {}): Observable<FlowSubmissionResponse> {
    return this.http.post<FlowSubmissionResponse>(`${this.apiUrl}/submit/${flowName}`, payload);
  }

  // Query run states using new API — returns RunStateDTO (worker execution state)
  queryRuns(names?: string[], last_n: number = 5): Observable<RunStateDTO[]> {
    const request: { names?: string[]; last_n?: number } = { last_n };
    if (names && names.length > 0) {
      request.names = names;
    }
    return this.http.post<RunStateDTO[]>(`${this.apiUrl}/runs/query`, request);
  }

  // Get list of runs with sorting and pagination
  getRuns(offset: number = 0, limit: number = 50): Observable<RunStateDTO[]> {
    return this.queryRuns(undefined, limit);
  }

   // Query logs using new API (requires both flow_name and run_id)
   queryLogs(flow_name: string, run_id: string, with_logs: boolean = true): Observable<RunDTO[]> {
     return this.http.post<RunDTO[]>(`${this.apiUrl}/logs/query`, { flow_name, run_id, with_logs });
   }

   // Get a specific run by run_id (backend looks up flow_name)
   getRunById(runId: string, with_logs: boolean = true): Observable<RunDTO> {
     return this.http.get<RunDTO>(`${this.apiUrl}/runs/${runId}?with_logs=${with_logs}`);
   }

   // Get a specific run with logs (requires flow_name)
   getRun(flowName: string, runId: string): Observable<RunDTO> {
     return this.queryLogs(flowName, runId, true).pipe(map(results => results[0]));
   }

  // Get dashboard metrics for the last 24 hours
  async getDashboardMetrics(): Promise<DashboardMetrics> {
    // Calculate timestamp for 24 hours ago
    const now = new Date();
    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    // Fetch all runs from last 24 hours
    const allRuns = await this.queryRuns(undefined, 100).toPromise() || [];

    // Filter runs from last 24 hours
    const last24hRuns = allRuns.filter(run => {
      const runStart = new Date(run.started_at);
      return runStart >= twentyFourHoursAgo;
    });

    // Calculate metrics
    const totalFlows = last24hRuns.length;
    const successfulFlows = last24hRuns.filter(r => r.status.toLowerCase() === 'completed').length;
    const failedFlows = last24hRuns.filter(r =>
      r.status.toLowerCase() === 'failed' ||
      r.status.toLowerCase() === 'error' ||
      r.status.toLowerCase() === 'critical'
    ).length;
    const runningFlows = last24hRuns.filter(r => r.status.toLowerCase() === 'running').length;

    const successRate = totalFlows > 0 ? (successfulFlows / totalFlows) * 100 : 0;
    const failureRate = totalFlows > 0 ? (failedFlows / totalFlows) * 100 : 0;

    // Get failed runs
    const failedRuns = last24hRuns.filter(r =>
      r.status.toLowerCase() === 'failed' ||
      r.status.toLowerCase() === 'error' ||
      r.status.toLowerCase() === 'critical'
    );

    // Get long-running flows (> 2 hours)
    const twoHoursInMs = 2 * 60 * 60 * 1000;
    const longRunningFlows = last24hRuns.filter(r => {
      if (!r.ended_at) return false;
      const durationMs = new Date(r.ended_at).getTime() - new Date(r.started_at).getTime();
      return durationMs > twoHoursInMs;
    });

    // Calculate total compute time
    let totalComputeMs = 0;
    last24hRuns.forEach(run => {
      if (run.ended_at) {
        totalComputeMs += new Date(run.ended_at).getTime() - new Date(run.started_at).getTime();
      }
    });
    const totalComputeTime = this.humanizeDuration(totalComputeMs);

    // Calculate average execution time
    const averageMs = totalFlows > 0 ? totalComputeMs / totalFlows : 0;
    const averageExecutionTime = this.humanizeDuration(averageMs);

    // Determine health status
    let healthStatus: 'green' | 'orange' | 'red';
    if (failedFlows > 0) {
      healthStatus = 'red';
    } else if (longRunningFlows.length > 0 || runningFlows > 0) {
      healthStatus = 'orange';
    } else {
      healthStatus = 'green';
    }

    return {
      healthStatus,
      totalFlows,
      successfulFlows,
      failedFlows,
      runningFlows,
      successRate,
      failureRate,
      failedRuns,
      longRunningFlows,
      totalComputeTime,
      averageExecutionTime,
      lastUpdated: new Date()
    };
  }

  // Helper: Parse duration string to milliseconds
  private parseDurationToMs(duration: string): number {
    // Duration format examples: "1.23s", "2m 34s", "1h 23m", "2d 5h"
    let totalMs = 0;

    // Match patterns like "2d", "5h", "23m", "1.23s"
    const dayMatch = duration.match(/(\d+(?:\.\d+)?)\s*d/);
    const hourMatch = duration.match(/(\d+(?:\.\d+)?)\s*h/);
    const minMatch = duration.match(/(\d+(?:\.\d+)?)\s*m/);
    const secMatch = duration.match(/(\d+(?:\.\d+)?)\s*s/);

    if (dayMatch) totalMs += parseFloat(dayMatch[1]) * 24 * 60 * 60 * 1000;
    if (hourMatch) totalMs += parseFloat(hourMatch[1]) * 60 * 60 * 1000;
    if (minMatch) totalMs += parseFloat(minMatch[1]) * 60 * 1000;
    if (secMatch) totalMs += parseFloat(secMatch[1]) * 1000;

    return totalMs;
  }

  // Helper: Convert milliseconds to human-readable duration
  private humanizeDuration(ms: number): string {
    if (ms === 0) return '0s';

    const days = Math.floor(ms / (24 * 60 * 60 * 1000));
    ms %= 24 * 60 * 60 * 1000;
    const hours = Math.floor(ms / (60 * 60 * 1000));
    ms %= 60 * 60 * 1000;
    const minutes = Math.floor(ms / (60 * 1000));
    ms %= 60 * 1000;
    const seconds = Math.floor(ms / 1000);

    const parts: string[] = [];
    if (days > 0) parts.push(`${days}d`);
    if (hours > 0) parts.push(`${hours}h`);
    if (minutes > 0) parts.push(`${minutes}m`);
    if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`);

    return parts.join(' ');
  }
}

// Dashboard metrics interface
export interface DashboardMetrics {
  healthStatus: 'green' | 'orange' | 'red';
  totalFlows: number;
  successfulFlows: number;
  failedFlows: number;
  runningFlows: number;
  successRate: number;
  failureRate: number;
  failedRuns: RunStateDTO[];
  longRunningFlows: RunStateDTO[];
  totalComputeTime: string;
  averageExecutionTime: string;
  lastUpdated: Date;
}
