import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface FlowListItem {
  name: string;
  last_status: string;
  finished_ago: string;
  duration: string;
  doc?: string;
}

export interface FlowRunInfo {
  flow_id: string;
  flow_name: string;
  started_at: string;
  finished_at?: string;
  status: string;
  error?: string;
}

export interface TaskRunInfo {
  task_id: string;
  task_name: string;
  flow_id: string;
  flow_name: string;
  started_at: string;
  finished_at?: string;
  status: string;
  result?: string;
  error?: string;
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
  runFlow(flowName: string, payload: StartFlowRequest = {}): Observable<FlowRunInfo> {
    return this.http.post<FlowRunInfo>(`${this.apiUrl}/flows/${flowName}/run`, payload);
  }

  // Get list of runs
  getRuns(offset: number = 0, limit: number = 50): Observable<FlowRunInfo[]> {
    return this.http.get<FlowRunInfo[]>(`${this.apiUrl}/runs`, {
      params: { offset: offset.toString(), limit: limit.toString() }
    });
  }

  // Get a specific run
  getRun(runId: string): Observable<FlowRunInfo> {
    return this.http.get<FlowRunInfo>(`${this.apiUrl}/runs/${runId}`);
  }

  // Get tasks for a run
  getRunTasks(runId: string): Observable<TaskRunInfo[]> {
    return this.http.get<TaskRunInfo[]>(`${this.apiUrl}/runs/${runId}/tasks`);
  }
}
