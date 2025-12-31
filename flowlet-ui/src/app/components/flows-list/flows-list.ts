import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { FlowletApi, RunSummaryDTO } from '../../services/flowlet-api';
import { forkJoin } from 'rxjs';

interface FlowWithStatus {
  name: string;
  status: string | null;
  finished_ago: string | null;
  duration: string | null;
  doc: string | null;
}

@Component({
  selector: 'app-flows-list',
  imports: [CommonModule, RouterModule, MatTableModule, MatButtonModule, MatProgressSpinnerModule, MatChipsModule, MatIconModule, MatSnackBarModule],
  templateUrl: './flows-list.html',
  styleUrl: './flows-list.css',
})
export class FlowsList implements OnInit {
  flows: FlowWithStatus[] = [];
  loading = true;
  error: string | null = null;
  displayedColumns: string[] = ['name', 'status', 'finished', 'duration', 'description', 'actions'];

  constructor(private flowletApi: FlowletApi, private snackBar: MatSnackBar) {}

  ngOnInit(): void {
    this.loadFlows();
  }

  loadFlows(): void {
    this.loading = true;
    this.error = null;

    // Fetch both available flows and latest run summaries
    forkJoin({
      flows: this.flowletApi.getFlows(),
      runs: this.flowletApi.queryRuns({
        query: {
          type: 'pipe',
          queries: [
            {
              type: 'sort',
              key: { type: 'get', keys: ['span_id'] },
              reverse: true
            },
            {
              type: 'get',
              keys: [{ _type: 'slice', start: 0, stop: 5, step: 1 }]
            }
          ]
        }
      })
    }).subscribe({
      next: ({ flows, runs }) => {
        // Create a map of latest runs by flow name
        const runsByFlow = new Map<string, RunSummaryDTO>();
        runs.forEach(run => {
          if (!runsByFlow.has(run.span_name)) {
            runsByFlow.set(run.span_name, run);
          }
        });

        // Merge flows with their latest run status
        this.flows = flows.map(flow => ({
          name: flow.name,
          status: runsByFlow.get(flow.name)?.status || null,
          finished_ago: this.calculateTimeAgo(runsByFlow.get(flow.name)?.end_ts),
          duration: runsByFlow.get(flow.name)?.duration || null,
          doc: flow.docstring || null
        }));

        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load flows: ' + err.message;
        this.loading = false;
      }
    });
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

  runFlow(flowName: string): void {
    this.flowletApi.runFlow(flowName).subscribe({
      next: () => {
        this.snackBar.open(`Flow "${flowName}" started successfully`, 'Close', { duration: 3000 });
        this.loadFlows();
      },
      error: (err) => {
        this.snackBar.open('Failed to start flow: ' + err.message, 'Close', { duration: 5000 });
      }
    });
  }

  getStatusColor(status: string): 'primary' | 'accent' | 'warn' | undefined {
    switch(status) {
      case 'completed': return 'primary';
      case 'failed': return 'warn';
      case 'running': return 'accent';
      default: return undefined;
    }
  }
}
