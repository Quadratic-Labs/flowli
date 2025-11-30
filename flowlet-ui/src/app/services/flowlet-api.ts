import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

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

export interface StartFlowRequest {
  kwargs?: { [key: string]: any };
}

@Injectable({
  providedIn: 'root'
})
export class FlowletApi {
  // Use proxy in development (/api) or direct URL in production
  private apiUrl = '/api';

  constructor(private http: HttpClient) {}

  // Get list of flows
  getFlows(): Observable<FlowListItem[]> {
    return this.http.get<FlowListItem[]>(`${this.apiUrl}/flows`);
  }

  // Run a flow
  runFlow(flowName: string, payload: StartFlowRequest = {}): Observable<void> {
    return this.http.post<void>(`${this.apiUrl}/flows/${flowName}/execute`, payload);
  }

  // Get list of runs
  getRuns(offset: number = 0, limit: number = 50): Observable<FlowRunInfo[]> {
    return this.http.get<FlowRunInfo[]>(`${this.apiUrl}/runs`, {
      params: { offset: offset.toString(), limit: limit.toString() }
    });
  }

  // Get a specific run
  getRun(runId: string): Observable<RunDetailInfo> {
    return this.http.get<RunDetailInfo>(`${this.apiUrl}/runs/${runId}`);
  }
}
