import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { FlowletApi, RunSummaryDTO } from '../../services/flowlet-api';

interface RunDisplay {
  name: string;
  run_id: string;
  status: string;
  started_at: string;
  ended_at: string | null;
}

@Component({
  selector: 'app-runs-list',
  imports: [CommonModule, RouterModule, MatTableModule, MatButtonModule, MatProgressSpinnerModule, MatChipsModule, MatIconModule],
  templateUrl: './runs-list.html',
  styleUrl: './runs-list.css',
})
export class RunsList implements OnInit {
  runs: RunDisplay[] = [];
  loading = true;
  error: string | null = null;
  offset = 0;
  limit = 50;
  displayedColumns: string[] = ['name', 'status', 'started', 'finished', 'actions'];

  constructor(private flowletApi: FlowletApi) {}

  ngOnInit(): void {
    this.loadRuns();
  }

  loadRuns(): void {
    this.loading = true;
    this.error = null;
    this.flowletApi.getRuns(this.offset, this.limit).subscribe({
      next: (runs: RunSummaryDTO[]) => {
        // Map RunSummaryDTO to RunDisplay format for the template
        this.runs = runs.map(run => ({
          name: run.span_name,
          run_id: run.span_id,
          status: run.status,
          started_at: run.start_ts,
          ended_at: run.end_ts
        }));
        this.loading = false;
      },
      error: (err) => {
        this.error = 'Failed to load runs: ' + err.message;
        this.loading = false;
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
