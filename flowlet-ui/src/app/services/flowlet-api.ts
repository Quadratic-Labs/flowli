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

// Query request models
export interface RunQueryRequest {
  names?: string[] | null;
  query?: any | null;
}

export interface LogQueryRequest {
  runs?: string[] | null;
  query?: any | null;
}

export interface StartFlowRequest {
  kwargs?: { [key: string]: any };
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

  // Run a flow
  runFlow(flowName: string, payload: StartFlowRequest = {}): Observable<void> {
    return this.http.post<void>(`${this.apiUrl}/execute/${flowName}`, payload);
  }

  // Query run summaries using new API
  queryRuns(request: RunQueryRequest = {}): Observable<RunSummaryDTO[]> {
    return this.http.post<RunSummaryDTO[]>(`${this.apiUrl}/runs/query`, request);
  }

  // Get list of runs with sorting and pagination
  getRuns(offset: number = 0, limit: number = 50): Observable<RunSummaryDTO[]> {
    const query = {
      type: 'pipe',
      queries: [
        {
          type: 'sort',
          key: { type: 'get', keys: ['span_id'] },
          reverse: true
        },
        {
          type: 'get',
          keys: [{ _type: 'slice', start: offset, stop: offset + limit, step: 1 }]
        }
      ]
    };
    return this.queryRuns({ query });
  }

  // Query logs using new API
  queryLogs(request: LogQueryRequest = {}): Observable<RunDTO[]> {
    return this.http.post<RunDTO[]>(`${this.apiUrl}/logs/query`, request);
  }

  // Get a specific run with logs
  getRun(runId: string): Observable<RunDTO> {
    return this.http.post<RunDTO[]>(`${this.apiUrl}/logs/query`, {
      runs: [runId]
    }).pipe(
      // Extract the first (and only) result
      map((results: RunDTO[]) => {
        if (results.length === 0) {
          throw new Error(`Run ${runId} not found`);
        }
        return results[0];
      })
    );
  }
}
