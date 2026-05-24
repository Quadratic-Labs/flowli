import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';
import { FlowletApi, RunStateDTO } from '../../services/flowlet-api';
import { BreadcrumbService } from '../../services/breadcrumb.service';
import { forkJoin } from 'rxjs';

interface FlowWithStatus {
  name: string;
  status: string | null;
  recent_statuses: string[];
  finished_ago: string | null;
  duration: string | null;
  doc: string | null;
}

@Component({
  selector: 'app-flows-list',
  imports: [CommonModule, RouterModule, MatTableModule, MatButtonModule, MatProgressSpinnerModule, MatIconModule],
  templateUrl: './flows-list.html',
  styleUrl: './flows-list.css',
})
export class FlowsList implements OnInit {
  flows: FlowWithStatus[] = [];
  loading = true;
  error: string | null = null;
  displayedColumns: string[] = ['name', 'status', 'finished', 'duration', 'description', 'actions'];

  constructor(
    private flowletApi: FlowletApi,
    private breadcrumbService: BreadcrumbService
  ) {}

  ngOnInit(): void {
    this.breadcrumbService.setBreadcrumbs([
      { label: 'Home', url: '/' },
      { label: 'Flows', url: '/flows' }
    ]);
    this.loadFlows();
  }

  loadFlows(): void {
    this.loading = true;
    this.error = null;

    // Fetch both available flows and recent run summaries (more runs to get history per flow)
    forkJoin({
      flows: this.flowletApi.getFlows(),
      runs: this.flowletApi.getRuns(0, 100)
    }).subscribe({
      next: ({ flows, runs }) => {
        // Group runs by flow name and keep the last 5 for each flow
        const runsByFlow = new Map<string, RunStateDTO[]>();
        runs.forEach(run => {
          if (!runsByFlow.has(run.flow_name)) {
            runsByFlow.set(run.flow_name, []);
          }
          const flowRuns = runsByFlow.get(run.flow_name)!;
          if (flowRuns.length < 5) {
            flowRuns.push(run);
          }
        });

        // Merge flows with their run history
        this.flows = flows.map(flow => {
          const flowRuns = runsByFlow.get(flow.name) || [];
          const latestRun = flowRuns[0];
          const duration = latestRun?.ended_at
            ? this.humanizeDurationMs(new Date(latestRun.ended_at).getTime() - new Date(latestRun.started_at).getTime())
            : null;

          return {
            name: flow.name,
            status: latestRun?.status || null,
            recent_statuses: flowRuns.map(run => run.status),
            finished_ago: this.calculateTimeAgo(latestRun?.ended_at ?? undefined),
            duration,
            doc: flow.docstring || null
          };
        });

        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load flows: ' + err.message;
        this.loading = false;
      }
    });
  }

  private humanizeDurationMs(ms: number): string {
    if (ms < 1000) return `${ms}ms`;
    const s = Math.floor(ms / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ${s % 60}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
  }

  private calculateTimeAgo(timestamp: string | undefined): string | null {
    if (!timestamp) return null;

    const now = new Date();
    const then = new Date(timestamp);
    const diffMs = now.getTime() - then.getTime();
    const diffSecs = Math.floor(diffMs / 1000);

    if (diffSecs < 60) return `${diffSecs}s ago`;
    const diffMins = Math.floor(diffSecs / 60);
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
  }


  getStatusAtIndex(statuses: string[], index: number): string | null {
    // Right-align the statuses: placeholders on the left, actual runs on the right
    // If we have 2 runs, they should be at index 3 and 4, with placeholders at 0, 1, 2
    const totalSlots = 5;
    const offset = totalSlots - statuses.length;

    if (index < offset) {
      return null; // Placeholder
    }

    // Reverse the array so oldest is on the left and newest on the right
    const reversed = [...statuses].reverse();
    return reversed[index - offset];
  }

  getStatusColor(status: string): string {
    const lowerStatus = status?.toLowerCase() || '';

    // Green for success/completed
    if (lowerStatus.includes('completed') || lowerStatus.includes('success')) {
      return '#4caf50'; // Green
    }

    // Red for failed/error/critical
    if (lowerStatus.includes('failed') || lowerStatus.includes('error') || lowerStatus.includes('critical')) {
      return '#f44336'; // Red
    }

    // Yellow/Orange for running/warning/pending
    if (lowerStatus.includes('running') || lowerStatus.includes('warning') || lowerStatus.includes('pending')) {
      return '#ff9800'; // Orange
    }

    // Gray for unknown/N/A
    return '#9e9e9e'; // Gray
  }
}
